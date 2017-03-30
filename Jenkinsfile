#!groovy

node {
  def sbtHome = tool 'default-sbt'

  def SBT = "${sbtHome}/bin/sbt -Dsbt.log.noformat=true"

  def branch = env.BRANCH_NAME

  echo "current branch is ${branch}"

  checkout scm

  stage('Cleanup') {
    sh "${SBT} clean"
    echo "in cleanup"
  }

  stage('Build') {
    sh "${SBT} compile"
    echo "in build"
  }

  stage('Test') {
    //sh "${SBT} \"serene-core/test-only au.csiro.data61.core.DatasetRestAPISpec\" || true"
    //sh "${SBT} \"serene-core/test-only au.csiro.data61.core.ModelRestAPISpec\" || true"
    docker.image('jenkins-1').inside {
      // sh "sbt serene-core/test || true"
      sh "$sbt \"serene-core/test-only au.csiro.data61.core.DatasetRestAPISpec\" || true"
      echo "serene-core test done"
    }
  }

  step([$class: 'JUnitResultArchiver', testResults: '**/core/target/test-reports/*.xml'])
}