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
package org.bdgenomics.mango.models

import net.liftweb.json.Serialization.write
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ VariantContext, ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.projections.{ GenotypeField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.{ VariantContextRDD, DatabaseVariantAnnotationRDD }
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, VariantAnnotation }
import org.bdgenomics.mango.tiling._
import org.bdgenomics.mango.util.Bookkeep
import org.bdgenomics.mango.layout.{ GenotypeJson, VariantJson, Coverage }
import org.bdgenomics.adam.projections.DatabaseVariantAnnotationField

import scala.reflect.ClassTag

/*
 * Handles loading and tracking of data from persistent storage into memory for Genotype data.
 * @see LazyMaterialization.scala
 */
class VariantMaterialization(s: SparkContext,
                             filePaths: List[String],
                             d: SequenceDictionary,
                             chunkS: Int) extends Serializable {

  @transient val sc = s
  @transient implicit val formats = net.liftweb.json.DefaultFormats
  val dict = d
  val chunkSize = chunkS
  val bookkeep = new Bookkeep(chunkSize)
  val files = filePaths
  var data: VariantContextRDD = VariantContextRDD(sc.emptyRDD[VariantContext], dict, Seq())

  def getGenotypes() = {

  }

  /* If the RDD has not been initialized, initialize it to the first get request
    * Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
    * If it exists, call get on the IntervalRDD
    * Otherwise call put on the sections of data that don't exist
    * Here, ks, is an option of list of personids (String)
    */
  def get(region: ReferenceRegion,
          alleleCount: Option[Int] = None,
          alleleFrequency: Option[Float] = None): (String, String) = {

    val seqRecord = dict(region.referenceName)
    seqRecord match {
      case Some(_) => {
        val regionsOpt = bookkeep.getMaterializedRegions(region, files)
        if (regionsOpt.isDefined) {
          for (r <- regionsOpt.get) {
            put(r)
          }
        }
        val x = data.rdd.filter(r => {
          if (r.databases.isDefined) {
            println(r.databases.getOrElse("none"))
            (r.databases.get.getThousandGenomesAlleleCount > alleleCount.getOrElse(-2) && r.databases.get.getThousandGenomesAlleleFrequency > alleleFrequency.getOrElse(-2.0F))
          } else true
        })
        stringify(VariantContextRDD(x, dict, data.samples))
      } case None => {
        throw new Exception("Not found in dictionary")
      }
    }
  }

  def put(region: ReferenceRegion) = {
    val newData = VariantMaterialization.load(sc, Some(region), files, dict)
    data = VariantContextRDD(data.rdd.union(newData.rdd), dict, newData.samples)
    bookkeep.rememberValues(region, files)
  }

  def stringify(data: VariantContextRDD): (String, String) = {
    val results = data.rdd.map(r => (r.variant, r.genotypes, r.databases))
      .map(r => (VariantJson(r._1, r._3), r._2.toArray))
      .collect
    // map variants and genotypes to json
    val variants = write(results.map(r => r._1))
    val genotypes = write(
      results.map(r => r._2)
        .filter(r => !r.isEmpty)
        .map(r => GenotypeJson(r)))
    (variants, genotypes)
  }
}

object VariantMaterialization {

  /**
   * Loads variant annotations in from vcf or parquet file
   * @param sc SparkContext
   * @param region ReferenceRegion
   * @param files vcf or adam file to load
   * @param sd SequenceDictionary for file
   * @return RDD of DatabaseVariantAnnotationss
   */
  def load(sc: SparkContext, region: Option[ReferenceRegion], files: List[String], sd: SequenceDictionary): VariantContextRDD = {

    val x: Array[VariantContextRDD] = files.map(fp => {
      // TODO: support through adam
      if (!fp.endsWith(".vcf")) {
        throw UnsupportedFileException("File type not supported")
      }
      // TODO: replace with load indexed vcf and include ReferenceRegion
      sc.loadVcf(fp)
    }).toArray
    val samples = x.flatMap(_.samples).toSeq
    VariantContextRDD(sc.union(x.map(_.rdd)), sd, samples)
  }
}