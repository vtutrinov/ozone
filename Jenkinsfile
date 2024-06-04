@Library("sdp-pipelines@develop")
@Library('ru.sbrf.devsecops@master')

import ru.sber.Variables

import javax.lang.model.element.VariableElement

def artifactVersion = "1.3.0"
Variables.componentVersion = artifactVersion

def componentName = "ozone"
Variables.componentName = componentName

def snapshotVersion = params.isSnapshot ? "-SNAPSHOT" : ""
def componentVersionSnapshot = "${Variables.componentVersion}${snapshotVersion}"

properties([
  parameters([
    choice(
            name: "stackName",
            choices: Variables.stackNames,
            description: "SDP or HDP stack"
    ),
    choice(
            name: "stackVersion",
            choices: Variables.stackVersions,
            description: "Stack version"
    ),
    string(
            name: "stackBuildId",
            defaultValue: Variables.stackBuildId,
            description: "Stack build id"
    ),
    booleanParam(
            name: "skipTests",
            defaultValue: true,
            description: "Skip or not"
    ),
    booleanParam(
            name: "skipBuildStage",
            defaultValue: false,
            description: "Skip or not. Do not change without confidence"
    ),
    booleanParam(
            name: "isSnapshot",
            defaultValue: true,
            description: "Is SNAPSHOT or not."
    ),
    booleanParam(
            name: "skipArtefactDeploy",
            defaultValue: true,
            description: "Skip or not."
    ),
    booleanParam(
            name: "SastCheckmarxOnly",
            defaultValue: false,
            description: "Run SAST Chekmarks without build"
    ),
    booleanParam(
            name: "updateParams",
            defaultValue: false,
            description: "Dry run, no changes, like 2 at https://dev.to/pencillr/jenkins-pipelines-and-their-dirty-secrets-2"
    )
   ])
])


pipeline {
    agent { node { label "sdp || sdp-ansible-runner" } }
    triggers {
        pollSCM(env.BRANCH_NAME == 'sdp-develop' ? 'H/5 * * * *' : '')
    }
    options {
        ansiColor("xterm")
        // convention is here: https://sbtatlas.sigma.sbrf.ru/wiki/pages/viewpage.action?pageId=75932050
        buildDiscarder logRotator(
                artifactDaysToKeepStr: "7",
                artifactNumToKeepStr: "10",
                daysToKeepStr: "7",
                numToKeepStr: "50"
        )
        disableConcurrentBuilds()
        durabilityHint "PERFORMANCE_OPTIMIZED"
        skipStagesAfterUnstable()
    }
    stages {
        stage("Update params") {
            when {
                anyOf {
                    equals expected: true, actual: params.updateParams
                    equals expected: 1, actual: currentBuild.number
                }
            }
            steps {
                script {
                    sdpStageUpdateParams()
                }
            }
        }
        stage("Prepare") {
            when { equals expected: false, actual: Variables.updateParams }
            steps {
                script {
                    sdpStagePrepare()
                    artifactVersion = "${Variables.componentVersion}.${params.stackVersion}-${params.stackBuildId}${snapshotVersion}"
                    Variables.stackVersion = params.stackVersion
                    Variables.dockerArgs.add("--env HOME=/var/cache")
                    Variables.buildDir = "./hadoop-ozone/dist/target/ozone-${artifactVersion}"

                    artifactVersionNoSnapshot = "${Variables.componentVersion}.${params.stackVersion}-${params.stackBuildId}"
                }
            }
        }
        stage("SAST-Checkmarx") {
            when {
                allOf {
                    equals expected: false, actual: params.updateParams
                    equals expected: true, actual: params.SastCheckmarxOnly
                    }
                }
            steps {
                script {
                    sdpStageSastCheckmarks()
                }
            }
        }
        stage("Bump version") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                configFileProvider([configFile(fileId: 'maven-settings-sberosc', targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                    script {
                        docker.image(Variables.dockerImages["${Variables.cpuArch}"]["ozone"]).inside(Variables.dockerArgs.join(" ")) {
                            sh script: """
                                mvn -B versions:set                              \
                                -DprocessAllModules=true                         \
                                -DnewVersion=${artifactVersion}                  \
                                -s ${MAVEN_SETTINGS}                             \
                                -Duser.home=${Variables.dockerCacheMount}
                            """
                            sh script: """
                                find . -name pom.xml -type f | xargs sed -i "s/$componentVersionSnapshot/$artifactVersion/g"
                            """
                        }
                    }
                }
            }
        }
        stage("Build") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: false, actual: Variables.skipBuildStage
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                script {
                    // https://sbt-jenkins.sigma.sbrf.ru/job/SDP/configfiles/index
                    configFileProvider([configFile(fileId: 'maven-settings-sberosc', targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                        script {
                            docker.image(Variables.dockerImages["${Variables.cpuArch}"]["ozone"]).inside(Variables.dockerArgs.join(" ")) {
                                sh script: """
                                    mvn clean install                                \
                                    -Duser.home=${Variables.dockerCacheMount}        \
                                    -s ${MAVEN_SETTINGS}                             \
                                    -DskipTests=${Variables.skipTests}               \
                                    -DskipDocs=true
                                """
                            }
                        }
                    }
                }
            }
        }
        stage("Deploy artifacts") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: true, actual: Variables.skipTests
                    equals expected: false, actual: params.skipArtefactDeploy
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                script {
                    configFileProvider([configFile(fileId: 'sdp-maven-settings', targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                        docker.image(Variables.dockerImages["${Variables.cpuArch}"]["ozone"]).inside(Variables.dockerArgs.join(" ")) {
                            sh script: """
                                mvn -B deploy -s ${MAVEN_SETTINGS}                  \
                                -Duser.home=${Variables.dockerCacheMount}           \
                                -DskipTests=${Variables.skipTests}
                            """
                        }
                    }
                }
            }
        }
        stage("Install ozone") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                script {
                    def stackBuildRoot = "${Variables.buildRootDir}/usr/${params.stackName}/${params.stackVersion}-${params.stackBuildId}"
                    def stackRealRoot = "/usr/${params.stackName}/${params.stackVersion}-${params.stackBuildId}"

                    if (Variables.skipBuildStage) {
                        sh script: "rm -rf ${stackBuild}"
                    }

                    sh script: """
                        mkdir -p "${Variables.buildRootDir}"

                        mkdir -p "${Variables.buildRootDir}/var/lib/ozone"

                        mkdir -p "${Variables.buildRootDir}/var/log/ozone"

                        mkdir -p "${Variables.buildRootDir}/usr/bin"
                        mkdir -p "${Variables.buildRootDir}/usr/lib"
                        mkdir -p "${Variables.buildRootDir}/usr/include"
                        mkdir -p "${Variables.buildRootDir}/usr/libexec/shellprofile.d"
                        mkdir -p "${Variables.buildRootDir}/usr/share/man/man1"

                        mkdir -p "${Variables.buildRootDir}/etc/default"
                        mkdir -p "${Variables.buildRootDir}/etc/bash_completion.d"

                        mkdir -p "${stackBuildRoot}/ozone/etc/hadoop/conf.empty"
                        mkdir -p "${stackBuildRoot}/ozone/bin"
                        mkdir -p "${stackBuildRoot}/ozone/etc"
                        mkdir -p "${stackBuildRoot}/ozone/client"
                        mkdir -p "${stackBuildRoot}/ozone/man/man1"
                        mkdir -p "${stackBuildRoot}/ozone/sbin"
                        mkdir -p "${stackBuildRoot}/ozone/lib/native"
                        mkdir -p "${stackBuildRoot}/ozone/libexec/"
                        mkdir -p "${stackBuildRoot}/ozone/share/ozone/byteman/"
                        mkdir -p "${stackBuildRoot}/ozone/share/ozone/classpath/"
                        mkdir -p "${stackBuildRoot}/ozone/share/ozone/lib/native/"


                        mkdir -p "${stackBuildRoot}/etc/default/hadoop-httpfs"
                        mkdir -p "${stackBuildRoot}/etc/bash_completion.d"
                        mkdir -p "${stackBuildRoot}/etc/security/limits.d/"

                        mkdir -p "${stackBuildRoot}/usr/bin"
                        mkdir -p "${stackBuildRoot}/usr/lib"
                        mkdir -p "${stackBuildRoot}/usr/include"

                   """
                   // copy the executables
                   sh script: """
                        cp -a ${Variables.buildDir}/bin/ozone ${stackBuildRoot}/ozone/bin/
                        cp -a ${Variables.buildDir}/sbin/*.sh ${stackBuildRoot}/ozone/sbin/
                        cp -a ${Variables.buildDir}/share/ozone/classpath/* ${stackBuildRoot}/ozone/share/ozone/classpath/
                        cp -a ${Variables.buildDir}/share/ozone/byteman/* ${stackBuildRoot}/ozone/share/ozone/byteman/
                        cp -a ${Variables.buildDir}/share/ozone/lib/*.jar ${stackBuildRoot}/ozone/share/ozone/lib/
                        cp -r -a ${Variables.buildDir}/libexec/* ${stackBuildRoot}/ozone/libexec/
                        cp -r ${Variables.buildDir}/etc/hadoop/* ${stackBuildRoot}/ozone/etc/hadoop/conf.empty
                   """

                    //Apache Ozone client
                    Variables.buildDir = "./hadoop-ozone/client/target"
                    stackBuildRoot = "${Variables.buildRootDir}/usr/${params.stackName}/${params.stackVersion}-${params.stackBuildId}"
                    stackRealRoot = "/usr/${params.stackName}/${params.stackVersion}-${params.stackBuildId}"

                    sh script: """
                        mkdir -p "${Variables.buildRootDir}"
                        mkdir -p "${stackBuildRoot}/ozone-client/lib"
                    """

                    sh script: """
                        cp -a ${Variables.buildDir}/ozone-client-${artifactVersion}.jar ${stackBuildRoot}/ozone-client/lib/ozone-client-${artifactVersionNoSnapshot}.jar
                    """

                }
            }
        }
        stage("Package") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                script {
                    sdpStagePackage()
                }
            }
        }
        stage("Deploy") {
            when {
                allOf {
                    equals expected: false, actual: Variables.updateParams
                    equals expected: false, actual: params.SastCheckmarxOnly
                }
            }
            steps {
                script {
                    sdpStageDeploy()
                }
            }
        }
    }
    post {
        always {
            script {
                sdpStagePostNotify()
            }
        }
    }
}
