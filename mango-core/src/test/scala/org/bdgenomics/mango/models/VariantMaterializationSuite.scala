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

import net.liftweb.json._

import net.liftweb.json.DefaultFormats
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.mango.layout.{ GenotypeJson, VariantJson }
import org.bdgenomics.mango.util.MangoFunSuite

class VariantMaterializationSuite extends MangoFunSuite {
  implicit val formats = DefaultFormats

  val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L),
    SequenceRecord("chrM", 20000L),
    SequenceRecord("20", 90000L)))

  // test vcf data'
  val vcfFile1 = resourcePath("truetest.vcf")
  val vcfKey1 = LazyMaterialization.filterKeyFromFile("truetest.vcf")
  val vcfFile2 = resourcePath("truetest2.vcf")
  val vcfKey2 = LazyMaterialization.filterKeyFromFile("truetest2.vcf")

  val vcfFiles = List(vcfFile1, vcfFile2)
  val key = LazyMaterialization.filterKeyFromFile("truetest.vcf")
  val key2 = LazyMaterialization.filterKeyFromFile("truetest2.vcf")

  // test reference data
  var referencePath = resourcePath("mm10_chrM.fa")

  sparkTest("Fetch from 1 vcf file") {
    val region = new ReferenceRegion("chrM", 0, 999)
    val data = new VariantMaterialization(sc, List(vcfFile1), sd, 1000)
    val json = data.get(region)

    val variants = parse(json._1).extract[Array[VariantJson]]
    val genotypes = parse(json._2).extract[Array[GenotypeJson]]
    assert(variants.length == 3)
    assert(genotypes.length == 3)
  }

  sparkTest("more than 1 vcf file") {
    val region = new ReferenceRegion("chrM", 0, 999)
    val data = new VariantMaterialization(sc, vcfFiles, sd, 10)
    val json = data.get(region)

    val variants = parse(json._1).extract[Array[VariantJson]]
    val genotypes = parse(json._2).extract[Array[GenotypeJson]]
    assert(variants.length == 6)
    assert(genotypes.length == 6)
  }

}