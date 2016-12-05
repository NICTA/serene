package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.features._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.nlptools.distance._

import edu.cmu.lti.ws4j._

import java.io._

/**
 *  This file generates a matrix of wordnet distances using WS4j.
 **/
object RunDistanceMatrixGenerator {
    val usageMessage = """Usage:
                         #    java RunDistanceMatrixGenerator <outputDir>
                       """.stripMargin('#')

    def weightedNWDist(distfunc: (String, String) => Double): (String, String) => Double = {
        (s1: String, s2: String) => {
            val dist = distfunc(s1,s2)
            dist * (s1.length + s2.length).toDouble/2.0
        }
    }

    def main(args: Array[String]) = {
        if(args.size < 1) {
            println(usageMessage)
        } else {
            //create output folder if it doesn't exist
            val outputDir = args(0)
            val dirFile = new File(outputDir)
            if(!dirFile.exists) {
                dirFile.mkdir()
            }

            abstract class NamedDataLoader(val domainName: String) {
                def loadDataSets(): List[DataModel]
                def loadLabels(): Labels
            }

            //specify datasets we want
            val allDataSetLoaders = List(
                // (new NamedDataLoader("wisc-realestate") with WISCRealEstateDomain1Utils)
                (new NamedDataLoader("citycouncil-data") with OpenGovCityCouncilUtils)
            )

            //load all datasets and labels
            println("Loading datasets...")
            val allDataSets = allDataSetLoaders.map({loader => (
                loader.domainName, 
                loader.loadDataSets(), 
                loader.loadSemanticTypeLabels()
            )})
            println("Datasets loaded.")

            //specify the distances we want computed
            val featureExtractors = List(
                // ("WS4j_LIN", WS4JWordNetDistanceMetric(WS4J.runLIN _).computeDistance _),
                ("EditDist", weightedNWDist(OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance")))
            )

            val allAttrs = allDataSets.flatMap({case (name, database, labels) => 
                database.flatMap({datatable => 
                    DataModel.getAllAttributes(datatable)
                })
            })

            //compute distances
            val distMatrices = featureExtractors.par.map({case (distname, distFun) =>
                (distname, (("id" :: allAttrs.map({_.id})).mkString(",") :: allAttrs.map({a1 =>
                    (a1.id :: allAttrs.map({a2 =>
                        distFun(a1.metadata.get.name, a2.metadata.get.name)
                    })).mkString(",")
                })).mkString("\n"))
            })

            //save results to file
            distMatrices.foreach({case (name, matrix) =>
                val dbdir = new File(outputDir)
                if(!dbdir.exists) {
                    dbdir.mkdir
                }
                var out = new PrintWriter(new File(s"$outputDir/$name.csv"))
                out.println(matrix)
                out.close
            })

            println("DONE!")
        }
    }
}