# Contributing Guide

Thank you for your interest in contributing to this project!

## Development Workflow

1. **Fork** the repository and create a feature branch:
   ```bash
   git checkout -b feature/<short-description>
   ```

2. **Install** dependencies and set up your local environment:
   ```bash
   pip install -r requirements.txt
   dbt deps
   ```

3. **Make your changes** following the conventions below.

4. **Test** your changes locally:
   ```bash
   dbt run --select <your_model>
   dbt test --select <your_model>
   ```

5. **Commit** with a descriptive message following [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   feat(silver): add silver_address model with deduplication
   fix(gold): correct null handling in gold_sales_summary
   docs: update ARCHITECTURE.md with Unity Catalog section
   ```

6. Open a **Pull Request** against `main` and fill in the PR template.

---

## Code Conventions

### SQL / dbt Models

- Use **snake_case** for all identifiers.
- Prefix models with the layer name: `bronze_`, `silver_`, `gold_`.
- Always add a `{{ config(...) }}` block at the top of every model.
- Every model must have a corresponding entry in its layer's `_models.yml` file.
- Add `not_null` and `unique` tests for all primary/surrogate key columns.
- Use CTEs instead of nested subqueries.
- Alias all derived columns with meaningful names.

### Python (Notebooks)

- Follow [PEP 8](https://peps.python.org/pep-0008/) style.
- Type-annotate all function signatures.
- Use `logging` instead of `print()` for diagnostic output.
- Keep notebook cells small and focused on a single logical unit.

### Documentation

- Document **every** new model in its layer YAML file with a `description:`.
- Document every column that has business significance.
- Update `ARCHITECTURE.md` if the data flow changes.

---

## Pull Request Checklist

Before submitting a PR, ensure:

- [ ] `dbt run --select <model>` completes without errors
- [ ] `dbt test --select <model>` passes all tests
- [ ] Model is documented in the corresponding `_models.yml`
- [ ] No hardcoded credentials, connection strings, or secrets
- [ ] Commit messages follow Conventional Commits
- [ ] ARCHITECTURE.md updated if data flow changes

---

## Reporting Issues

Open a GitHub Issue with:
- A clear title describing the problem
- Steps to reproduce
- Expected vs. actual behaviour
- Relevant error messages or log output
