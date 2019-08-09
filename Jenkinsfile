#!groovy

pipeline {
    agent { label 'master' }

    stages {
        stage('clean') {
            steps {
                sh 'git clean -fdx'
            }
        }
        stage('set_version') {
            steps {
                sh 'bumpversion patch'
            }
        }
        stage('release') {
            when { branch 'master' }
            steps {
                withCredentials([usernamePassword(credentialsId: env.CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                    sh '''
                        export VERSION=$(bump2version --list --allow-dirty release | grep new_version= | sed -r s,"^.*=",,)
                        git push origin master
                        git push origin refs/tags/v$VERSION
                    '''
                }
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
