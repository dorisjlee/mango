/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.mango.cli

import java.io.{ File, FileNotFoundException }
import net.liftweb.json.Serialization.write
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bdgenomics.mango.converters.GA4GHConverter
import org.bdgenomics.adam.models.{ SequenceRecord, ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Genotype }
import org.bdgenomics.mango.core.util.VizUtils
import org.bdgenomics.mango.layout.{ VariantLayout, VariantFreqLayout }
import org.bdgenomics.mango.tiling.{ L0, Layer }
import org.bdgenomics.mango.models.{ FeatureMaterialization, GenotypeMaterialization, AlignmentRecordMaterialization, ReferenceMaterialization }
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.Logging
import org.ga4gh.{ GASearchReadsResponse, GAReadAlignment }
import org.fusesource.scalate.TemplateEngine
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.scalatra._

object VizTimers extends Metrics {
  //HTTP requests
  val ReadsRequest = timer("GET reads")
  val FreqRequest = timer("GET frequency")
  val VarRequest = timer("GET variants")
  val VarFreqRequest = timer("Get variant frequency")
  val FeatRequest = timer("GET features")
  val AlignmentRequest = timer("GET alignment")

  //RDD operations
  val FreqRDDTimer = timer("RDD Freq operations")
  val VarRDDTimer = timer("RDD Var operations")
  val FeatRDDTimer = timer("RDD Feat operations")
  val RefRDDTimer = timer("RDD Ref operations")
  val GetPartChunkTimer = timer("Calculate block chunk")

  //Generating Json
  val MakingTrack = timer("Making Track")
  val DoingCollect = timer("Doing Collect")
  val PrintReferenceTimer = timer("JSON get reference string")
}

object VizReads extends BDGCommandCompanion with Logging {

  val commandName: String = "viz"
  val commandDescription: String = "Genomic visualization for ADAM"
  implicit val formats = net.liftweb.json.DefaultFormats

  var sc: SparkContext = null
  var partitionCount: Int = 0
  var referencePath: String = ""
  var readsPaths: Option[List[String]] = None
  var variantsPaths: Option[List[String]] = None
  var featurePaths: Option[List[String]] = None
  var sampNames: Option[List[String]] = None
  var readsExist: Boolean = false
  var variantsExist: Boolean = false
  var featuresExist: Boolean = false
  var globalDict: SequenceDictionary = null
  var refRDD: ReferenceMaterialization = null
  var readsData: AlignmentRecordMaterialization = null
  var variantData: GenotypeMaterialization = null
  var featureData: FeatureMaterialization = null
  var server: org.eclipse.jetty.server.Server = null
  var screenSize: Int = 1000
  var chunkSize: Long = 5000
  var readsLimit: Int = 10000

  // HTTP ERROR RESPONSES
  object errors {
    var outOfBounds = NotFound("Region Out of Bounds")
    var largeRegion = RequestEntityTooLarge("Region too large")
    var unprocessableFile = UnprocessableEntity("File type not supported")
    var notFound = NotFound("File not found")
  }

  def apply(cmdLine: Array[String]): BDGCommand = {
    new VizReads(Args4j[VizReadsArgs](cmdLine))
  }

  /**
   * Returns stringified version of sequence dictionary
   *
   * @param dict: dictionary to format to a string
   * @return List of sequence dictionary strings of form referenceName:0-referenceName.length
   */
  def formatDictionaryOpts(dict: SequenceDictionary): String = {
    dict.records.map(r => r.name + ":0-" + r.length).mkString(",")
  }

  //Correctly shuts down the server
  def quit() {
    val thread = new Thread {
      override def run() {
        try {
          log.info("Shutting down the server")
          server.stop()
          log.info("Server has stopped")
        } catch {
          case e: Exception => {
            log.info("Error when stopping Jetty server: " + e.getMessage, e)
          }
        }
      }
    }
    thread.start()
  }

}

case class ReferenceJson(reference: String, position: Long)

class VizReadsArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "reference", usage = "The reference file to view, required", index = 0)
  var referencePath: String = null

  @Args4jOption(required = false, name = "-preprocess_path", usage = "Path to file containing reference regions to be preprocessed")
  var preprocessPath: String = null

  @Args4jOption(required = false, name = "-repartition", usage = "The number of partitions")
  var partitionCount: Int = 0

  @Args4jOption(required = false, name = "-read_files", usage = "A list of reads files to view, separated by commas (,)")
  var readsPaths: String = null

  @Args4jOption(required = false, name = "-var_files", usage = "A list of variants files to view, separated by commas (,)")
  var variantsPaths: String = null

  @Args4jOption(required = false, name = "-feat_files", usage = "The feature files to view, separated by commas (,)")
  var featurePaths: String = null

  @Args4jOption(required = false, name = "-port", usage = "The port to bind to for visualization. The default is 8080.")
  var port: Int = 8080

  @Args4jOption(required = false, name = "-test", usage = "The port to bind to for visualization. The default is 8080.")
  var testMode: Boolean = false
}

class VizServlet extends ScalatraServlet {
  implicit val formats = net.liftweb.json.DefaultFormats

  get("/?") {
    redirect("/overall")
  }

  get("/quit") {
    VizReads.quit()
  }

  get("/overall") {
    contentType = "text/html"
    val templateEngine = new TemplateEngine
    // set initial referenceRegion so it is defined
    session("referenceRegion") = ReferenceRegion("chr", 1, 100)
    templateEngine.layout("mango-cli/src/main/webapp/WEB-INF/layouts/overall.ssp",
      Map("dictionary" -> VizReads.formatDictionaryOpts(VizReads.globalDict),
        "readsSamples" -> VizReads.sampNames,
        "readsExist" -> VizReads.readsExist,
        "variantsExist" -> VizReads.variantsExist,
        "variantsPaths" -> VizReads.variantsPaths,
        "featuresExist" -> VizReads.featuresExist))
  }

  // Sends byte array to front end
  get("/reference/:ref") {
    val viewRegion = ReferenceRegion(params("ref"), params("start").toLong,
      VizUtils.getEnd(params("end").toLong, VizReads.globalDict(params("ref"))))
    session("referenceRegion") = viewRegion
    Ok(write(VizReads.refRDD.getReferenceString(viewRegion)))
  }

  get("/sequenceDictionary") {
    Ok(write(VizReads.refRDD.dict.records))
  }

  get("/reads/:ref") {
    VizTimers.ReadsRequest.time {
      val viewRegion = ReferenceRegion(params("ref"), params("start").toLong, params("end").toLong)
      contentType = "json"
      val dictOpt = VizReads.globalDict(viewRegion.referenceName)

      // determines whether to wait for reference to complete calculation
      val wait =
        try {
          params("wait").toBoolean
        } catch {
          case e: Exception => false
        }
      // wait for reference to finish to avoid race condition to reference string
      if (wait) {
        var stopWait = false
        while (!stopWait) {
          stopWait = viewRegion.equals(session.get("referenceRegion").get)
          Thread sleep 20
        }
      }
      if (dictOpt.isDefined && viewRegion.end <= dictOpt.get.length) {
        val sampleIds: List[String] = params("sample").split(",").toList
        val data =
          if (viewRegion.length() < VizReads.readsLimit) {
            val isRaw =
              try {
                params("isRaw").toBoolean
              } catch {
                case e: Exception => false
              }
            val layer: Option[Layer] =
              if (isRaw) Some(VizReads.readsData.rawLayer)
              else None
            VizReads.readsData.multiget(viewRegion, sampleIds, layer)
          } else {
            // Large Region. just return read frequencies
            VizReads.readsData.getFrequency(viewRegion, sampleIds)
          }
        Ok(data)
      } else VizReads.errors.outOfBounds
    }
  }

  get("/GA4GHreads/:ref") {
    VizTimers.ReadsRequest.time {
      val viewRegion = ReferenceRegion(params("ref"), params("start").toLong, params("end").toLong)
      contentType = "json"

      // if region is in bounds of reference, return data
      val dictOpt = VizReads.globalDict(viewRegion.referenceName)
      if (dictOpt.isDefined && viewRegion.end <= dictOpt.get.length) {
        val sampleIds: List[String] = params("sample").split(",").toList
        val alignments: RDD[AlignmentRecord] = VizReads.readsData.getAlignments(viewRegion, sampleIds)
        // convert to GA4 GH avro and build a response
        val gaReads: List[GAReadAlignment] = alignments.map(GA4GHConverter.toGAReadAlignment)
          .collect
          .toList
        val readResponse = GASearchReadsResponse.newBuilder()
          .setAlignments(gaReads)
          .build()

        // write response
        Ok(readResponse.toString)
      } else VizReads.errors.outOfBounds
    }
  }

  get("/variants/:ref") {
    VizTimers.VarRequest.time {

      val viewRegion = ReferenceRegion(params("ref"), params("start").toLong, params("end").toLong)
      contentType = "json"

      // if region is in bounds of reference, return data
      val dictOpt = VizReads.globalDict(viewRegion.referenceName)

      if (dictOpt.isDefined && viewRegion.end <= dictOpt.get.length) {
        val data =
          if (viewRegion.length() < 10000) {
            val isRaw = true
            val layer: Option[Layer] =
              if (isRaw) Some(VizReads.variantData.rawLayer)
              else None
            //Always fetches the frequency, but also additionally fetches raw data if necessary
            val samples = VizReads.variantData.fileMap.keys.toList
            VizReads.variantData.multiget(viewRegion, samples, layer)
          } else VizReads.errors.outOfBounds
        Ok(data)
      }
    }
  }

  get("/testReads/:ref") {
    val bamFile = VizReads.readsPaths.get.head
    val region = ReferenceRegion("chrM", 0, 5)
    val alignments = AlignmentRecordMaterialization.load(VizReads.sc, region, bamFile).filter(_.getStart > 3)
    val gaReads: List[GAReadAlignment] = alignments.map(GA4GHConverter.toGAReadAlignment)
      .collect
      .toList
    val readResponse = GASearchReadsResponse.newBuilder()
      .setAlignments(gaReads)
      .build()

    // write response
    Ok(readResponse.toString)
  }

  get("/features/:ref") {
    if (!VizReads.featuresExist)
      VizReads.errors.notFound
    else {
      val viewRegion =
        ReferenceRegion(params("ref"), params("start").toLong,
          VizUtils.getEnd(params("end").toLong, VizReads.globalDict(params("ref"))))
      val regionSize = viewRegion.width
      if (regionSize > L0.maxSize) {
        VizReads.errors.largeRegion
      } else {
        VizTimers.FeatRequest.time {
          val dictOpt = VizReads.globalDict(viewRegion.referenceName)
          // if region is in bounds of reference, return data
          if (dictOpt.isDefined && viewRegion.end <= dictOpt.get.length) {
            val data = VizReads.featureData.get(viewRegion)
            Ok(data)
          } else VizReads.errors.outOfBounds
        }
      }
    }
  }
}

class VizReads(protected val args: VizReadsArgs) extends BDGSparkCommand[VizReadsArgs] with Logging {
  val companion: BDGCommandCompanion = VizReads

  override def run(sc: SparkContext): Unit = {
    VizReads.sc = sc

    VizReads.partitionCount =
      if (args.partitionCount <= 0)
        VizReads.sc.defaultParallelism
      else
        args.partitionCount

    // initialize all datasets
    initReference
    initAlignments
    initVariants
    initFeatures

    // run preprocessing if preprocessing file was provided
    preprocess

    // start server
    if (!args.testMode) startServer()

    /*
     * Initialize required reference file
     */
    def initReference() = {
      val referencePath = Option(args.referencePath)

      VizReads.referencePath = {
        referencePath match {
          case Some(_) => referencePath.get
          case None    => throw new FileNotFoundException("reference file not provided")
        }
      }
      val chunkSize = 1000 // TODO: configurable?
      VizReads.refRDD = new ReferenceMaterialization(sc, VizReads.referencePath, chunkSize)
      VizReads.chunkSize = VizReads.refRDD.chunkSize
      VizReads.globalDict = VizReads.refRDD.getSequenceDictionary
    }

    /*
     * Initialize loaded alignment files
     */
    def initAlignments = {
      VizReads.readsData = new AlignmentRecordMaterialization(sc, (VizReads.chunkSize).toInt, VizReads.refRDD)
      val readsPaths = Option(args.readsPaths)
      if (readsPaths.isDefined) {
        VizReads.readsPaths = Some(args.readsPaths.split(",").toList)
        VizReads.readsExist = true
        VizReads.sampNames = Some(VizReads.readsData.init(VizReads.readsPaths.get))
      }
    }

    /*
     * Initialize loaded variant files
     */
    def initVariants() = {
      VizReads.variantData = GenotypeMaterialization(sc, VizReads.globalDict, VizReads.partitionCount)
      val variantsPaths = Option(args.variantsPaths)
      if (variantsPaths.isDefined) {
        VizReads.variantsPaths = VizReads.variantData.init(args.variantsPaths.split(",").toList)
        VizReads.variantsExist = true
      }
    }

    /*
     * Initialize loaded feature files
     */
    def initFeatures() = {
      val featuresPath = Option(args.featurePaths)
      featuresPath match {
        case Some(_) => {
          // filter out incorrect file formats
          VizReads.featurePaths = Some(args.featurePaths.split(",").toList
            .filter(path => path.endsWith(".bed") || path.endsWith(".adam")))

          // warn for incorrect file formats
          args.featurePaths.split(",").toList
            .filter(path => !path.endsWith(".bed") && !path.endsWith(".adam"))
            .foreach(file => log.warn(s"{file} does is not a valid feature file. Removing... "))

          if (!VizReads.featurePaths.get.isEmpty) {
            VizReads.featuresExist = true
            VizReads.featureData = new FeatureMaterialization(sc, VizReads.featurePaths.get, VizReads.globalDict, (VizReads.chunkSize).toInt)
          } else VizReads.featuresExist = false
        }
        case None => {
          VizReads.featuresExist = false
          log.info("No features file provided")
        }
      }
    }

    /*
     * Preloads data specified in optional text file int the format Name, Start, End where Name is
     * the chromosomal location, start is start position and end is end position
     */
    def preprocess = {
      val path = Option(args.preprocessPath)
      if (path.isDefined && VizReads.sampNames.isDefined) {
        val file = new File(path.get)
        if (file.exists()) {
          val lines = scala.io.Source.fromFile(path.get).getLines
          lines.foreach(r => {
            val line = r.split(",")
            try {
              val region = ReferenceRegion(line(0), line(1).toLong, line(2).toLong)
              VizReads.readsData.multiget(region,
                VizReads.sampNames.get)
            } catch {
              case e: Exception => log.warn("preprocessing file requires format referenceName,start,end")
            }
          })
        }
      }
    }

    /*
     * Starts server once on startup
     */
    def startServer() = {
      VizReads.server = new org.eclipse.jetty.server.Server(args.port)
      val handlers = new org.eclipse.jetty.server.handler.ContextHandlerCollection()
      VizReads.server.setHandler(handlers)
      handlers.addHandler(new org.eclipse.jetty.webapp.WebAppContext("mango-cli/src/main/webapp", "/"))
      VizReads.server.start()
      println("View the visualization at: " + args.port)
      println("Quit at: /quit")
      VizReads.server.join()
    }

  }
}

case class Record(name: String, length: Long)

