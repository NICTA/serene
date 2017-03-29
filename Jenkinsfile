#!groovy

node {
  def sbtHome = tool 'default-sbt'

  def SBT = "${sbtHome}/bin/sbt -Dsbt.log.noformat=true"

  def branch = env.BRANCH_NAME

  echo "current branch is ${branch}"

  checkout scm

  stage('Cleanup') {
    //sh "${SBT} clean"
    echo "in cleanup"
  }

  stage('Build') {
    // sh "${SBT} compile"
    echo "in build"
  }

  stage('Test') {
    sh "${SBT} serene-core/test"
    sh "${SBT} \"serene-core/test-only au.csiro.data61.core.SSDStorageSpec\""
    echo "serene-core test done"
    junit '**/core/target/test-reports/*.xml'
}

}