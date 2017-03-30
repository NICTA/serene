#!groovy

node {
  def sbtHome = tool 'default-sbt'

  def SBT = "${sbtHome}/bin/sbt -Dsbt.log.noformat=true"

  def branch = env.BRANCH_NAME

  echo "current branch is ${branch}"

  checkout scm

  stage('Cleanup') {
    docker.image('jenkins-scala-2.11.8').withRun('-u root') {
      sh "sbt clean"
    }
  }

  stage('Build') {
    docker.image('jenkins-scala-2.11.8').withRun('-u jenkins') {
      sh "/usr/bin/sbt compile"
    }
  }

  stage('Test') {
    //sh "${SBT} \"serene-core/test-only au.csiro.data61.core.DatasetRestAPISpec\" || true"
    //sh "${SBT} \"serene-core/test-only au.csiro.data61.core.ModelRestAPISpec\" || true"

    docker.image('jenkins-scala-2.11.8').withRun('-u jenkins') {

      // sh "sbt serene-core/test || true"

      sh "/usr/bin/sbt \"serene-core/test-only au.csiro.data61.core.DatasetRestAPISpec\" || true"
      echo "serene-core test done"
    }
  }

  step([$class: 'JUnitResultArchiver', testResults: '**/core/target/test-reports/*.xml'])
}