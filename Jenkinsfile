#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2024-12'

def repositoryName = 'knime-python-google-ads'

library "knime-pipeline@$BN"

properties([
    parameters(knimetools.getPythonExtensionParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultPythonExtensionBuild()

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
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result)
}
