pipeline {
    // Docker needed to run tests.
    agent { label 'docker' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('vault')}:${tool('sbt')}:$PATH"
        // Some wiring is broken between the custom-tools plugin and
        // the pipeline plugin which prevents these vars from being
        // injected when pulling in the custom 'vault' tool.
        VAULT_ADDR = 'https://clotho.broadinstitute.org:8200'
        VAULT_TOKEN_PATH = '/etc/vault-token-monster'
    }
    stages {
        stage('Check formatting') {
            steps {
                sh 'sbt scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt Compile/compile Test/compile IntegrationTest/compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" test'
            }
        }
        stage('Integration test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" IntegrationTest/test'
            }
        }
        stage('Publish') {
            when {
                anyOf {
                    branch 'master'
                    tag pattern: 'v\\d.*', comparator: 'REGEXP'
                }
            }
            environment {
                PATH = "${tool('gcloud')}:$PATH"
            }
            steps {
                script {
                    def saVaultKey = 'secret/dsde/monster/dev/gcr/broad-dsp-gcr-public-sa.json'
                    def saTmp = '${WORKSPACE}/sa-key.json'

                    def steps = [
                        '#!/bin/bash',
                        'set +x -euo pipefail',
                        'echo Publishing artifacts...',
                        // Pull the login key for the service account that can publish to GCR.
                        'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH)',
                        "vault read -format=json $saVaultKey | jq .data > $saTmp",
                        // Jenkins' home directory is read-only, so we have to set up
                        // local storage to write temporary gcloud / docker configs.
                        'export CLOUDSDK_CONFIG=${WORKSPACE}/.gcloud',
                        'export DOCKER_CONFIG=${WORKSPACE}/.docker',
                        'mkdir -p ${CLOUDSDK_CONFIG} ${DOCKER_CONFIG}',
                        'cp -r ${HOME}/.docker/* ${DOCKER_CONFIG}/',
                        // Log into gcloud, then link Docker to gcloud.
                        "gcloud auth activate-service-account \$(vault read -field=client_email $saVaultKey) --key-file=$saTmp",
                        'gcloud auth configure-docker --quiet',
                        // Push :allthethings: if the setup succeeded.
                        'sbt publish'
                    ]

                    sh steps.join('\n')
                }
            }
        }
    }
    post {
        always {
            junit '**/target/test-reports/*'
        }
        success {
            script {
                def vaultPath = 'secret/dsde/monster/dev/codecov/clinvar-ingest'
                def codecov = [
                        '1>&2 bash <(curl -s https://codecov.io/bash)',
                        '-K',
                        '-t ${CODECOV_TOKEN}',
                        "-s ${env.WORKSPACE}/target",
                        '-C $(git rev-parse HEAD)',
                        "-b ${env.BUILD_NUMBER}"
                ].join(' ')
                def parts = [
                        '#!/bin/bash',
                        'set +x',
                        'echo Publishing code coverage...',
                        'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH)',
                        "CODECOV_TOKEN=\$(vault read -field=token $vaultPath)",
                        'sbt coverageAggregate',
                        "if [ -z '${env.CHANGE_ID}' ]; then ${codecov} -B ${env.BRANCH_NAME}; else ${codecov} -P ${env.CHANGE_ID}; fi"
                ]
                sh parts.join('\n')
            }
        }
        cleanup {
            cleanWs()
        }
    }
}
