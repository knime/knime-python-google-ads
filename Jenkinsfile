#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2024-06'

def repositoryName = "knime-python-google-ads"

def extensionPath = "." // Path to knime.yml file
def outputPath = "output"

library "knime-pipeline@$BN"

knimeVersion = KNIMEConstants.getAPReleaseForBranch(BN)

properties([
    parameters(
        [p2Tools.getP2pruningParameter()] + \
        workflowTests.getConfigurationsAsParameters() + \
        condaHelpers.getForceCondaBuildParameter() + \
        condaHelpers.getGenerateSBOMForCondaParameter()
        ),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    timeout(time: 100, unit: 'MINUTES') {
        node('workflow-tests && ubuntu22.04 && java17') {
            stage("Checkout Sources") {
                env.lastStage = env.STAGE_NAME
                checkout scm
            }

            stage("Create Conda Env"){
                env.lastStage = env.STAGE_NAME
                prefixPath = "${WORKSPACE}/${repositoryName}"
                condaHelpers.createCondaEnv(prefixPath: prefixPath, pythonVersion:'3.9', packageNames: ["knime-extension-bundling=${knimeVersion}", 'jake'])
            }
            stage("Build Python Extension") {
                env.lastStage = env.STAGE_NAME
                // Handle Conda Parameters
                force_conda_build = params?.FORCE_CONDA_BUILD ? "--force-new-timestamp" : ""
                buildSboms = ""
                if (params.SBOM_FOR_CONDA) {
                    binaryFile = condaHelpers.downloadAndPrepareCycloneDX()
                    buildSboms = "--generate-sbom --cyclonedx-binary ${binaryFile}"
                }
                def buildPath = "build"

                withMavenJarsignerCredentials(options: [artifactsPublisher(disabled: true)], skipJarsigner: false) {
                    withEnv([ "MVN_OPTIONS=-Dknime.p2.repo=https://jenkins.devops.knime.com/p2/knime/" ]) {
                        withCredentials([usernamePassword(credentialsId: 'ARTIFACTORY_CREDENTIALS', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_LOGIN'),
                        ]) {
                            sh """
                            micromamba run -p ${prefixPath} build_python_extension.py ${extensionPath} ${outputPath} --render-folder ${buildPath} -f --knime-build --excluded-files ${prefixPath} ${force_conda_build} ${buildSboms}
                            """
                        }
                        if (params.SBOM_FOR_CONDA) {
                            echo "Sending SBOMs to OWASP"
                            knimeYML = readYaml file: 'knime.yml'
                            projectVersion = knimeYML.version
                            owasp.sendCondaSBOMs(projectVersion)
                        }
                    }
                }
            }
            stage("Deploy p2") {
                    env.lastStage = env.STAGE_NAME
                    p2Tools.deploy(outputPath)
                    println("Deployed")
                    try {
                        build job: "ap-composites/${env.BRANCH_NAME.replace('/', '%2F')}", parameters: [
                            string(name: 'REPOSITORY', value: repositoryName)
                        ], wait: false
                    } catch (ex) {
                        // Happens if ap-composites doesn't have a corresponding branch
                        echo ex.toString()
                    }
                }
            workflowTests.runTests(
                dependencies: [
                    repositories: [
                        'knime-base',
                        'knime-conda',
                        'knime-core-columnar',
                        'knime-credentials-base',
                        'knime-filehandling',
                        'knime-gateway',
                        'knime-google',
                        'knime-javasnippet',
                        'knime-json',
                        'knime-python',
                        'knime-python-types',
                        'knime-python-legacy',
                        'knime-python-bundling',
                        'knime-testing-internal',
                        'knime-productivity-oss',
                        'knime-reporting',
                        repositoryName
                        ],
                ],
            )
        }
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result)
}
