
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
package org.bdgenomics.mango.layout

import org.bdgenomics.adam.models.{ Exon, Gene, ReferenceRegion }
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, Genotype, Variant }

/**
 * This file contains case classes for json conversions
 */

case class Interval(start: Long, end: Long)

object VariantJson {
  def apply(v: Variant, d: Option[DatabaseVariantAnnotation] = None): VariantJson = {
    val (alleleCount: Int, alleleFrequency: Float) =
      if (d.isDefined)
        (d.get.getThousandGenomesAlleleCount, d.get.getThousandGenomesAlleleFrequency)
      else (-1, -1.0F)
    new VariantJson(v.getContigName, v.getStart, v.getReferenceAllele, v.getAlternateAllele, alleleCount, alleleFrequency)
  }
}
case class VariantJson(contig: String, position: Long, ref: String, alt: String, alleleCount: Int, alleleFrequency: Float)

object GenotypeJson {
  /**
   * Takes in an iterable of Genotypes from VariantContext and formats them to json
   * @param g Iterable of genotypes
   * @return 1 GenotypeJson object
   */
  def apply(g: Array[Genotype], d: Option[DatabaseVariantAnnotation] = None): GenotypeJson = {
    if (g.isEmpty) throw new Exception("Cannot format empty Genotypes to json")
    else {
      val (alleleCount: Int, alleleFrequency: Float) =
        if (d.isDefined)
          (d.get.getThousandGenomesAlleleCount, d.get.getThousandGenomesAlleleFrequency)
        else (-1, -1.0F)
      val samples = g.map(_.getSampleId)
      val first = g.head
      val variant = VariantJson(
        first.getContigName,
        first.getStart,
        first.getVariant.getReferenceAllele,
        first.getVariant.getAlternateAllele, alleleCount, alleleFrequency)
      GenotypeJson(samples, variant)
    }
  }
}
case class GenotypeJson(sampleIds: Array[String], variant: VariantJson)

case class BedRowJson(id: String, featureType: String, contig: String, start: Long, stop: Long)

object GeneJson {
  def apply(rf: Gene): Iterable[GeneJson] = {
    val transcripts = rf.transcripts
    transcripts.map(t => GeneJson(t.region, t.id, t.strand, Interval(t.region.start, t.region.end), t.exons, t.geneId, t.names.mkString(",")))
  }
}

case class GeneJson(position: ReferenceRegion, id: String, strand: Boolean, codingRegion: Interval,
                    exons: Iterable[Exon], geneId: String, name: String)

