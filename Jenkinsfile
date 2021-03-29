@Library('factual-shared-libs') _

pipeline {
    options {
        disableConcurrentBuilds()
    }
    agent {
        kubernetes{
            yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: maven
    image: registry.prod.factual.com/maven-with-thrift:0.14.1
    command: ['cat']
    tty: true
"""
        }
    }
    triggers {
        pollSCM('H/5 * * * *')
    }
    stages {
        stage('Unit Tests') {
            steps {
                container(name: 'maven') {
                    withCredentials([file(credentialsId: 'artifactory-settings', variable: 'MAVEN_SETTINGS')]) {
                        sh 'mvn -s $MAVEN_SETTINGS -U -B clean test'
                    }
                }
            }
        }
        stage ('Deploy Maven') {
            when {
                branch 'master'
            }
            steps{
                container(name: 'maven') {
                    withCredentials([file(credentialsId: 'artifactory-settings', variable: 'MAVEN_SETTINGS')]) {
                        sh 'mvn -B -U -s $MAVEN_SETTINGS deploy -DskipTests'
                    }
                }
            }
        }
    }
}
