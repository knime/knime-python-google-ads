#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-12'

def repositoryName = 'knime-python-google-ads'

library "knime-pipeline@$BN"

properties([
    /*
    When changes occur in the upstream jobs (e.g., "knime-python"), this configuration 
    ensures that dependent jobs (e.g., "knime-python-google-ads") are automatically rebuilt.

    Example:
        upstream(
            'knime-abc/' + env.BRANCH_NAME.replaceAll('/', '%2F') +
            ', knime-xyz/' + env.BRANCH_NAME.replaceAll('/', '%2F')
        )
    */
    pipelineTriggers([
		upstream('knime-python/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
	]),
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
