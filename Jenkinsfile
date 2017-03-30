#!groovy

node {
  def sbtHome = tool 'default-sbt'

  def SBT = "${sbtHome}/bin/sbt -Dsbt.log.noformat=true"

  def branch = env.BRANCH_NAME

  echo "current branch is ${branch}"

  checkout scm

  stage('Cleanup') {
    docker.image('jenkins-scala-2.11.8') {
      sh "sbt clean"
    }
  }

  stage('Build') {
    docker.image('jenkins-scala-2.11.8') {
      sh "sbt compile"
    }
  }

  stage('Test') {

    // for a single test suite add the line below to the docker image line
    //sh "${SBT} \"serene-core/test-only au.csiro.data61.core.DatasetRestAPISpec\" || true"

    docker.image('jenkins-scala-2.11.8') {
      sh "sbt serene-core/test || true"
      echo "serene-core test done"
    }
  }

  step([$class: 'JUnitResultArchiver', testResults: '**/core/target/test-reports/*.xml'])
}