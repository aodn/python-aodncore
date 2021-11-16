#!groovy

pipeline {
    agent none

    stages {
        stage('container') {
            agent {
                dockerfile {
                    // PLEASE DO NOT USE THE BELOW ARGS AS THIS IS A SECURITY ISSUE. SEE https://github.com/aodn/issues/issues/1076 FOR DETAILS.
                    // args '-u 498:495 -v ${HOME}/bin:${HOME}/bin -v /var/run/docker.sock:/var/run/docker.sock'
                    args '-v ${HOME}/bin:${HOME}/bin'
                    // additionalBuildArgs '--build-arg BUILDER_UID=$(id -u) --build-arg DOCKER_GID=$(stat -c %g /var/run/docker.sock)'
                    additionalBuildArgs '--build-arg BUILDER_UID=$(id -u)'
                }
            }
            stages {
                stage('clean') {
                    steps {
                        sh 'git reset --hard'
                        sh 'git clean -xffd'
                    }
                }
                stage('set_version') {
                    when { not { branch "master" } }
                    steps {
                        sh './bumpversion.sh build'
                    }
                }
                stage('release') {
                    when { branch 'master' }
                    steps {
                        withCredentials([usernamePassword(credentialsId: env.GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            sh './bumpversion.sh release'
                        }
                    }
                }
                // Test stage commented out to allow builds to complete. See issue #1076 for details
                //stage('test') {
                //    steps {
                //        sh 'pip install --user -r test_requirements.txt'
                //        sh 'pytest'
                //    }
                //}
                stage('package') {
                    steps {
                        sh 'python setup.py bdist_wheel'
                    }
                }
		// Skip generate docs stage due to error. To be resolved later. See backlog item #3414 
                //stage('generate_docs') {
                //    steps {
                //        dir('sphinx') {
                //            sh 'make generate'
                //        }
                //    }
                //}
            }
            post {
                success {
                    dir('dist/') {
                        archiveArtifacts artifacts: '*.whl', fingerprint: true, onlyIfSuccessful: true
                    }
                    script {
                        if (env.BRANCH_NAME == 'master') {
                            dir('sphinx') {
                                withCredentials([usernamePassword(credentialsId: env.GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                                    sh 'make deploy'
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
