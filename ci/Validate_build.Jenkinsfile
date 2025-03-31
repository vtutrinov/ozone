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

def acceptanceTests = [
    // "EC",
    // "HA-secure",
    // "HA-unsecure",
    "MR",
    // "balancer",
    "cert-rotation",
    "compat-new",
    "compat-old",
    // "leadership",
    // "misc",
    // "s3a",
    // "secure",
    // "unsecure",
    // "upgrade"
]

STASH_NAME = "sources"

def integrationParallelStages = [:]
def acceptanceParallelStages = [:]

def acceptanceParallelTests(type, test) {
    return {
        node('sdpozonebuilder') {
            def workspace = pwd()
            sh """
                sudo rm -rf ${workspace}
                mkdir ${workspace}
            """
            unstash STASH_NAME
            sh "mkdir -p hadoop-ozone/dist/target"
            unstash 'ozone-bin'
            def mavenLocationInsideDocker = "/home/test/maven_settings.xml"
            sh """
                tar xzvf hadoop-ozone/dist/target/ozone*.tar.gz -C hadoop-ozone/dist/target
                rm hadoop-ozone/dist/target/ozone*.tar.gz
                pushd hadoop-ozone/dist/target/ozone-*
                sudo mkdir -p .aws && sudo chmod 777 .aws && sudo chown 1000 .aws
                popd
                sed -i 's#-DforceStdout#-DforceStdout -s ${mavenLocationInsideDocker}#g' ./hadoop-ozone/dev-support/checks/${type}.sh;
                docker save apache/ozone-runner:20230615-1 -o ozone_runner.tar
                docker save apache/hadoop:3.3.6 -o apache_hadoop.tar
                docker save apache/ozone-testkrb5:20230318-1 -o ozone_testkrb5.tar
            """
            try {
                configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                    sh """
                        docker run --rm -i --privileged ${Variables.dockerArgs.join(' ')} -v \$(pwd):/home/test -w /home/test -d --name ${test}-${GIT_COMMIT} ubuntu:dind
                        docker exec ${test}-${GIT_COMMIT} bash -c "set -x \
                            && sleep 10 \
                            && docker load -i ozone_runner.tar \
                            && docker load -i apache_hadoop.tar \
                            && docker load -i ozone_testkrb5.tar \
                            && export OZONE_VOLUME_OWNER=1000 \
                            && export KEEP_IMAGE=false \
                            && export MAVEN_OPTS='-Duser.home=/var/build-cache ${Variables.mavenDistributionManagementString}' \
                            && export OZONE_ACCEPTANCE_SUITE=${test} \
                            && ./hadoop-ozone/dev-support/checks/${type}.sh ${Variables.mavenDistributionManagementString} \
                                -Duser.home=${Variables.dockerCacheMount}       \
                                -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                -DnpmInheritsProxyConfigFromMaven=true          \
                                -DexcludedGroups=unhealthy,org.apache.ozone.test.UnhealthyTest \
                                -Dsurefire.rerunFailingTestsCount=5 -Dsurefire.fork.timeout=3600 \
                                -Dmaven.repo.local=/var/build-cache/.m2/repository \
                                -s ${mavenLocationInsideDocker}
                        "
                    """
                }
            } finally {
                sh """
                    docker stop ${test}-${GIT_COMMIT}
                    sudo rm -rf target/${test}
                    sudo mv target/${type} target/${test}
                """

                dir ("target") {
                    archiveArtifacts artifacts: "${test}/**",
                        allowEmptyArchive: true
                }

                patterns = [
                    "**/surefire-reports/**/*.xml",
                    "**/integration-test/**/*.xml",
                    "target/${test}/*.xml",
                ]

                patterns.each {
                    pattern -> anonymous: {
                        xmlFiles = findFiles(glob: pattern)
                        println(xmlFiles)
                        if (xmlFiles) {
                            junit testResults: pattern, allowEmptyResults: true
                        }
                    }
                }
                sh """
                    sudo chown -R jenkins:jenkins ${workspace}
                """
                deleteDir()
            }
        }
    }
}

def integrationParallelTests(test) {
    return {
        node('sdpozonebuilder') {
            sh """
                sudo rm -rf ${workspace}
                mkdir ${workspace}
            """
            unstash STASH_NAME
            sh "mkdir -p hadoop-ozone/dist/target"
            unstash 'ozone-bin'
            sh """
                tar xzvf hadoop-ozone/dist/target/ozone*.tar.gz -C hadoop-ozone/dist/target
                rm hadoop-ozone/dist/target/ozone*.tar.gz
            """
            try {
                docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                    configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                        sh script: """
                            export MAVEN_OPTS=""
                            export OZONE_REPO_CACHED=true
                            ./hadoop-ozone/dev-support/checks/integration.sh -P${test} ${Variables.mavenDistributionManagementString} \
                                -Duser.home=${Variables.dockerCacheMount}       \
                                -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                -DnpmInheritsProxyConfigFromMaven=true          \
                                -DexcludedGroups=unhealthy,org.apache.ozone.test.UnhealthyTest \
                                -Dsurefire.rerunFailingTestsCount=5 -Dsurefire.fork.timeout=3600 \
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
                            junit testResults: pattern, allowEmptyResults: true
                        }
                    }
                }
                deleteDir()
            }
        }
    }
}

integrationTests.each { test ->
    integrationParallelStages.put(test, integrationParallelTests(test))
}

acceptanceTests.each { test ->
    acceptanceParallelStages.put(test, acceptanceParallelTests("acceptance", test))
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
                    try {
                        sdpStagePrepare()
                    } catch (Exception e) {
                        echo 'Exception occurred: ' + e.toString()
                    }
                }
            }
        }
        stage("Build") {
            steps {
                configFileProvider([configFile(fileId: "${configFileMVN}", targetLocation: 'maven_settings.xml', variable: 'MAVEN_SETTINGS')]) {
                    script {
                        stash name: STASH_NAME, useDefaultExcludes: false
                        docker.image(Variables.dockerImages["${Variables.cpuArch}"]["${Variables.componentName}"]).inside(Variables.dockerArgs.join(" ")) {
                            sh script: """
                                #scl enable rh-nodejs6 -- npm config set registry http://10.53.69.15:4873/
                                ./hadoop-ozone/dev-support/checks/build.sh \
                                    -Pdist -Dskip.npx -Dskip.installnpx -Djavac.version=8 \
                                    ${Variables.mavenDistributionManagementString}  \
                                    -Duser.home=${Variables.dockerCacheMount}       \
                                    -DnpmRegistryUrl=http://10.53.69.15:4873/       \
                                    -DnpmInheritsProxyConfigFromMaven=true          \
                                    -Dmaven.repo.local=/var/cache/.m2/repository    \
                                    -s ${MAVEN_SETTINGS}
                            """
                            stash includes: 'hadoop-ozone/dist/target/ozone-*.tar.gz', excludes: '!hadoop-ozone/dist/target/ozone-*-src.tar.gz', name: 'ozone-bin'
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
                            parallel integrationParallelStages
                        }
                    }
                }
            }
        }
        stage('Acceptance Tests') {
            steps {
                script {
                    parallel acceptanceParallelStages
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
