@Library("sdp-pipelines@sdp-develop")

import ru.sber.Variables

import javax.lang.model.element.VariableElement

Variables.componentName = "ozone"

configFileMVN = "maven-central"

def integrationTests = [
    "client",
    "container",
    "filesystem",
    "hdds",
    "om",
    "ozone",
    "recon",
    "shell",
    "snapshot"
]

STASH_NAME = "sources"

def parallelStages = [:]

def parallelIntegrationTests(test) {
    return {
        node('sdpozonebuilder') {
            retry(3) {
                try {
                    deleteDir()
                    docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                        unstash STASH_NAME
                        configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                            sh script: """
                                export MAVEN_OPTS=""
                                export OZONE_REPO_CACHED=true
                                ./hadoop-ozone/dev-support/checks/integration.sh -P${test} ${Variables.mavenDistributionManagementString} \
                                    -Duser.home=${Variables.dockerCacheMount}       \
                                    -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                    -DnpmInheritsProxyConfigFromMaven=true          \
                                    -DexcludedGroups=unhealthy,org.apache.ozone.test.UnhealthyTest \
                                    -Dsurefire.rerunFailingTestsCount=3 \
                                    -s ${MAVEN_SETTINGS}
                            """
                        }
                    }
                } finally {
                    sh "mv target/integration target/${test}"
                    dir ("target") {
                        archiveArtifacts artifacts: "${test}/**",
                            allowEmptyArchive: true
                    }

                    patterns = [
                        '**/surefire-reports/**/*.xml',
                        '**/integration-test/**/*.xml',
                    ]

                    patterns.each {
                        pattern -> anonymous: {
                            xmlFiles = findFiles(glob: pattern)
                            if (xmlFiles) {
                                junit skipMarkingBuildUnstable: true, testResults: pattern, allowEmptyResults: true
                            }
                        }
                    }
                    deleteDir()
                }
            }
        }
    }
}

integrationTests.each { test ->
    parallelStages.put(test, parallelIntegrationTests(test))
}

properties([])

pipeline {
    agent { node { label "sdpozonebuilder" } }

    options {
        timeout(time: 5, unit: 'HOURS')
        ansiColor("xterm")
        // convention is here: https://sbtatlas.sigma.sbrf.ru/wiki/pages/viewpage.action?pageId=75932050
        buildDiscarder logRotator(
                artifactDaysToKeepStr: "7",
                artifactNumToKeepStr: "10",
                daysToKeepStr: "7",
                numToKeepStr: "50"
        )
        durabilityHint "PERFORMANCE_OPTIMIZED"
        skipStagesAfterUnstable()
        disableConcurrentBuilds abortPrevious: true
    }
    stages {
        stage("Prepare") {
            steps {
                script {
                    sdpStagePrepare()
                    stash name: STASH_NAME, useDefaultExcludes: false
                }
            }
        }
        stage("Build") {
            steps {
                configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                    script {
                        docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                            sh script: """
                                #scl enable rh-nodejs6 -- npm config set registry http://10.53.69.15:4873/
                                ./hadoop-ozone/dev-support/checks/build.sh \
                                    -Pdist -Dskip.npx -Dskip.installnpx -Djavac.version=8 \
                                    ${Variables.mavenDistributionManagementString}  \
                                    -Duser.home=${Variables.dockerCacheMount}       \
                                    -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                    -DnpmInheritsProxyConfigFromMaven=true          \
                                    -s ${MAVEN_SETTINGS}
                            """
                        }
                    }
                }
            }
        }
        stage('Tests') {
            parallel {
                stage("Checkstyle") {
                    steps {
                        configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                            script {
                                docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                                    sh script: """
                                        ./hadoop-ozone/dev-support/checks/checkstyle.sh ${Variables.mavenDistributionManagementString} \
                                            -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                            -DnpmInheritsProxyConfigFromMaven=true          \
                                            -Duser.home=${Variables.dockerCacheMount}       \
                                            -DexcludedGroups=unhealthy,org.apache.ozone.test.UnhealthyTest \
                                            -s ${MAVEN_SETTINGS}
                                    """
                                }
                            }
                        }
                    }
                    post {
                        always {
                            dir ("target") {
                                archiveArtifacts artifacts: "checkstyle/**",
                                    allowEmptyArchive: true
                            }
                        }
                    }
                }
                stage("Findbugs") {
                    steps {
                        configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                            script {
                                docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                                    sh script: """
                                        ./hadoop-ozone/dev-support/checks/findbugs.sh ${Variables.mavenDistributionManagementString}  \
                                            -Duser.home=${Variables.dockerCacheMount}       \
                                            -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                            -DnpmInheritsProxyConfigFromMaven=true          \
                                            -DexcludedGroups=unhealthy,org.apache.ozone.test.UnhealthyTest \
                                            -s ${MAVEN_SETTINGS}
                                    """
                                }
                            }
                        }
                    }
                    post {
                        always {
                            dir ("target") {
                                archiveArtifacts artifacts: "findbugs/**",
                                    allowEmptyArchive: true
                            }
                        }
                    }
                }
                stage("Integration tests") {
                    steps {
                        script {
                            parallel parallelStages
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
    }
}
