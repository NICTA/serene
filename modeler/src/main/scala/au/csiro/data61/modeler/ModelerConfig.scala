/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package au.csiro.data61.modeler

import java.nio.file.Paths

import au.csiro.data61.types.ModelingProperties
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

/**
  * This object loads in the configuration .conf
  * file and parses the values into fields.
  */
object ModelerConfig extends LazyLogging {

  private val conf = ConfigFactory.load()

  // directory where temporary Karma stuff resides...
  val KarmaDir = conf.getString("config.karma-dir")

  // default folder to store alignment graph
  val DefaultAlignmenDir = Paths.get(KarmaDir, "alignment-graph/").toString

  logger.debug(s"Karma storage dir $KarmaDir")

  val defaultModelingProps: String = {
    val filler = List.fill(90)("#").mkString("") + "\n"
    val f = "#\n"

    filler +
      f + "# Semantic Typing\n" + f + filler + "\n" + "train.on.apply.history=false\n" +
      "predict.on.apply.history=false\n\n" + filler + f + "# Alignment\n" + f + filler +
      "\n" + "# turning off the next two flags is equal to manual alignment\n" +
      "compatible.properties=true\n" + "ontology.alignment=false\n" +
      "knownmodels.alignment=true\n" + "\n" + filler + f + "# Graph Builder\n" +
      "# (the flags in this section will only take effect when the \"ontology.alignment\" is true)\n" +
      f + filler + "\n" + "thing.node=false\n" + "\n" + "node.closure=true\n" + "\n" +
      "properties.direct=true\n" + "properties.indirect=true\n" + "properties.subclass=true\n" +
      "properties.with.only.domain=true\n" + "properties.with.only.range=true\n" +
      "properties.without.domain.range=false\n" + "\n"+ filler + f +
      "# Prefixes\n" + f + filler + "\n" +
      "karma.source.prefix=http://isi.edu/integration/karma/sources/\n" +
      "karma.service.prefix=http://isi.edu/integration/karma/services/\n" +
      "default.property=http://schema.org/name\n" + "\n" + filler + f +
      "# Model Learner\n" + f + filler + "\n" +
      "learner.enabled=true\n\n" +
      "add.ontology.paths=false\n\n" + "mapping.branching.factor=50\n" +
      "num.candidate.mappings=10\n" + "topk.steiner.tree=10\n" +
      "multiple.same.property.per.node=false\n\n" +
      "# scoring coefficients, should be in range [0..1]\n" +
      "scoring.confidence.coefficient=1.0\n" +
      "scoring.coherence.coefficient=1.0\n" +
      "scoring.size.coefficient=0.5\n\n" + filler + f +
      "# Other Settings\n" + f + filler + "\n" +
      "models.display.nomatching=false\n" + "history.store.old=false\n"

  }

  /**
    * Create string for modeling properties of octopus.
    * This string corresponds to karma style.
    * @param octopusModelingProps
    * @return
    */
  def makeModelingProps(octopusModelingProps: ModelingProperties): Option[String] = {
      val filler = List.fill(90)("#").mkString("") + "\n"
      val f = "#\n"

      Some(filler + f + "# Semantic Typing\n" + f + filler + "\n" +
        "train.on.apply.history=false\n" +
        "predict.on.apply.history=false\n\n" +
        filler + f + "# Alignment\n" + f + filler +
        "\n" + "# turning off the next two flags is equal to manual alignment\n" +
        s"compatible.properties=${octopusModelingProps.compatibleProperties}\n" +
        s"ontology.alignment=${octopusModelingProps.ontologyAlignment}\n" +
        "knownmodels.alignment=true\n\n" +
        filler + f + "# Graph Builder\n" +
        "# (the flags in this section will only take effect when the \"ontology.alignment\" is true)\n" +
        f + filler + "\n" +
        s"thing.node=${octopusModelingProps.thingNode}\n\n" +
        s"node.closure=${octopusModelingProps.nodeClosure}\n\n" +
        s"properties.direct=${octopusModelingProps.propertiesDirect}\n" +
        s"properties.indirect=${octopusModelingProps.propertiesIndirect}\n" +
        s"properties.subclass=${octopusModelingProps.propertiesSubclass}\n" +
        s"properties.with.only.domain=${octopusModelingProps.propertiesWithOnlyDomain}\n" +
        s"properties.with.only.range=${octopusModelingProps.propertiesWithOnlyRange}\n" +
        s"properties.without.domain.range=${octopusModelingProps.propertiesWithoutDomainRange}\n\n" +
        filler + f + "# Prefixes\n" + f + filler + "\n" +
        "karma.source.prefix=http://isi.edu/integration/karma/sources/\n" +
        "karma.service.prefix=http://isi.edu/integration/karma/services/\n" +
        "default.property=http://schema.org/name\n\n" +
        filler + f + "# Model Learner\n" + f + filler + "\n" +
        "learner.enabled=true\n\n" +
        s"add.ontology.paths=${octopusModelingProps.addOntologyPaths}\n\n" +
        s"mapping.branching.factor=${octopusModelingProps.mappingBranchingFactor}\n" +
        s"num.candidate.mappings=${octopusModelingProps.numCandidateMappings}\n" +
        s"topk.steiner.tree=${octopusModelingProps.topkSteinerTrees}\n" +
        s"multiple.same.property.per.node=${octopusModelingProps.multipleSameProperty}\n\n" +
        "# scoring coefficients, should be in range [0..1]\n" +
        s"scoring.confidence.coefficient=${octopusModelingProps.confidenceWeight}\n" +
        s"scoring.coherence.coefficient=${octopusModelingProps.coherenceWeight}\n" +
        s"scoring.size.coefficient=${octopusModelingProps.sizeWeight}\n\n" +
        filler + f + "# Other Settings\n" + f + filler + "\n" +
        "models.display.nomatching=false\n" + "history.store.old=false\n"
      )
  }
}
