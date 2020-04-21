pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-commons/master", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("test: baseline (jdk8)") {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					label 'data'
					args '-u root -v /var/run/docker.sock:/var/run/docker.sock'
					// root but with no maven caching
				}
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			steps {
				sh 'rm -rf ?'
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci clean dependency:list test -Dsort -U -B'
				sh "chown -R 1001:1001 target"
			}
		}

		stage("Test other configurations") {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: baseline (jdk11)") {
					agent {
						docker {
							image 'adoptopenjdk/openjdk11:latest'
							label 'data'
							args '-u root -v /var/run/docker.sock:/var/run/docker.sock'
							// root but with no maven caching
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,java11 clean dependency:list test -Dsort -U -B'
						sh "chown -R 1001:1001 target"
					}
				}

				stage("test: baseline (jdk14)") {
					agent {
						docker {
							image 'adoptopenjdk/openjdk14:latest'
							label 'data'
							args '-u root -v /var/run/docker.sock:/var/run/docker.sock'
							// root but with no maven caching
						}
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						sh 'rm -rf ?'
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,java11 clean dependency:list test -Dsort -U -B'
						sh "chown -R 1001:1001 target"
					}
				}
			}
		}

		stage('Release to artifactory') {
			when {
				anyOf {
					branch 'master'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					label 'data'
					args '-v $HOME:/tmp/jenkins-home'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				sh 'rm -rf ?'
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,artifactory ' +
						'-Dartifactory.server=https://repo.spring.io ' +
						"-Dartifactory.username=${ARTIFACTORY_USR} " +
						"-Dartifactory.password=${ARTIFACTORY_PSW} " +
						"-Dartifactory.staging-repository=libs-snapshot-local " +
						"-Dartifactory.build-name=spring-data-r2dbc " +
						"-Dartifactory.build-number=${BUILD_NUMBER} " +
						'-Dmaven.test.skip=true clean deploy -U -B'
			}
		}

		stage('Publish documentation') {
			when {
				branch 'master'
			}
			agent {
				docker {
					image 'adoptopenjdk/openjdk8:latest'
					label 'data'
					args '-v $HOME:/tmp/jenkins-home'
				}
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,distribute ' +
						'-Dartifactory.server=https://repo.spring.io ' +
						"-Dartifactory.username=${ARTIFACTORY_USR} " +
						"-Dartifactory.password=${ARTIFACTORY_PSW} " +
						"-Dartifactory.distribution-repository=temp-private-local " +
						'-Dmaven.test.skip=true clean deploy -U -B'
			}
		}
	}

	post {
		changed {
			script {
				slackSend(
						color: (currentBuild.currentResult == 'SUCCESS') ? 'good' : 'danger',
						channel: '#spring-data-dev',
						message: "${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL}")
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
