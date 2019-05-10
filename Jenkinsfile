#!groovy

pipeline {
    agent { label 'agent' }

    stages {
        stage('clean') {
            steps {
                sh 'git clean -fdx'
            }
        }

        stage('container') {
            agent {
                dockerfile {
                    additionalBuildArgs '--build-arg BUILDER_UID=${JENKINS_UID:-9999}'
                    reuseNode true
                }
            }
            stages {
                stage('test') {
                    steps {
                        sh 'pip install --user -e . .[testing]'
                        sh 'python setup.py test'
                    }
                }
                stage('package') {
                    steps {
                        sh 'python setup.py bdist_wheel'
                    }
                }
            }
            post {
                success {
                    dir('dist/') {
                        archiveArtifacts artifacts: '*.whl', fingerprint: true, onlyIfSuccessful: true
                    }
                }
            }
        }
    }
}
