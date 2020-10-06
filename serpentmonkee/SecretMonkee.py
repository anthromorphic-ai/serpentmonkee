
class SecretMonkee:  # --------------------------------------------------------------------
    def __init__(self, secretclient, project_id, what_for=[]):
        self.secretclient = secretclient
        self.project_id = project_id
        self.secrets = {}
        self.whatFor = what_for
        self.getSecrets()

    def _addSecret(self, secretName, secret):
        self.secrets[secretName] = secret

    def getSecret(self, secretclient, project_id, secret_name):
        request = {
            "name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"}
        response = secretclient.access_secret_version(request)
        secret_string = response.payload.data.decode("UTF-8")
        return secret_string

    def getSecrets(self):
        """
        Gets the secrets for the given whatFors
            ['sql','neo','redis].

            USAGE:
            sm = SecretMonkee(secretclient, project_id, ['neo']).secrets
        """
        if 'sql' in self.whatFor:
            self._addSecret("db_user", self.getSecret(
                self.secretclient, self.project_id, "DB_USER"))
            self._addSecret("db_pass", self.getSecret(
                self.secretclient, self.project_id, "DB_PASS"))
            self._addSecret("db_name", self.getSecret(
                self.secretclient, self.project_id, "DB_NAME"))
            self._addSecret("cloud_sql_connection_name", self.getSecret(
                self.secretclient, self.project_id, "CLOUD_SQL_CONNECTION_NAME"
            ))

        if 'neo' in self.whatFor:
            self._addSecret("neo_uri", self.getSecret(
                self.secretclient, self.project_id, "DB_NEO_URI"))
            self._addSecret("neo_user", self.getSecret(
                self.secretclient, self.project_id, "DB_NEO_USER"))
            self._addSecret("neo_pass", self.getSecret(
                self.secretclient, self.project_id, "DB_NEO_PASS"))

        if 'redis' in self.whatFor:
            self._addSecret("redis_host", self.getSecret(
                self.secretclient, self.project_id, "REDISHOST"))
            self._addSecret("redis_port", self.getSecret(
                self.secretclient, self.project_id, "REDISPORT"))
        return self.secrets
