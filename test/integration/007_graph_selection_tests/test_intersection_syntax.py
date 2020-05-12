from test.integration.base import DBTIntegrationTest, use_profile


class TestGraphSelection(DBTIntegrationTest):

    @property
    def schema(self):
        return "graph_selection_tests_007"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test__postgres__same_model_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'users,users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_lack(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'emails,users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__tags_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:bi,tag:users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_triple_descending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '*,tag:bi,tag:users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_triple_ascending(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', 'tag:users,tag:bi,*'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_with_exclusion(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(['run', '--models', '+users_rollup_dependency,base_models+', '--exclude', '+emails_alt'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             'tag:bi,users_roll_up+'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_intersection_lack(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '@emails,@emails_alt'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_exclude_triple_intersection(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '--exclude',
             '*,tag:bi,tag:users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '@base_models,tag:base'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '@base_models,tag:base', '--exclude', '@users'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '@base_models,tag:base',
             '--exclude', '@users', '@tag:bi'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)

    @use_profile('postgres')
    def test__postgres__intersection_concat_exclude_intersection_concat(self):
        self.run_sql_file("seed.sql")

        results = self.run_dbt(
            ['run', '--models', 'tag:bi,@users', '@base_models,tag:base',
             '--exclude', '@users,users', '@users,base_models'])
        self.assertEqual(len(results), 1)

        self.assertTablesEqual("seed", "users")
        created_models = self.get_models_in_schema()
        self.assertFalse('users_rollup' in created_models)
        self.assertFalse('base_users' in created_models)
        self.assertFalse('emails' in created_models)
