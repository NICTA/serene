#!groovy

node {
  def sbtHome = tool 'default-sbt'

  def SBT = "${sbtHome}/bin/sbt -Dsbt.log.noformat=true"

  def branch = env.BRANCH_NAME

  echo "current branch is ${branch}"

  checkout scm

  stage('Cleanup') {
    sh "${SBT} clean"
  }

  stage('Build') {
    sh "${SBT} compile"
  }

  stage('Test') {
    sh "${SBT} serene-core/test"
    junit '**/core/target/test-reports/*.xml'
}

}