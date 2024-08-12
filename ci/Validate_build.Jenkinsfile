@Library("sdp-pipelines@sdp-develop")
@Library("ru.sbrf.devsecops@master")

import ru.sber.Variables

import javax.lang.model.element.VariableElement

Variables.componentName = "ozone"

def snapshotVersion = params.isSnapshot ? "-SNAPSHOT" : ""
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
        node('sdp') {
            deleteDir()
            docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                unstash STASH_NAME
                configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                    sh script: """
                        export MAVEN_OPTS=""
                        ./hadoop-ozone/dev-support/checks/integration.sh -P${test} ${Variables.mavenDistributionManagementString} \
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

integrationTests.each { test ->
    parallelStages.put(test, parallelIntegrationTests(test))
}

properties([])

pipeline {
    agent { node { label "sdp" } }
    options {
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
                                            -s ${MAVEN_SETTINGS}
                                    """
                                }
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
                                            -s ${MAVEN_SETTINGS}
                                    """
                                }
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
}
