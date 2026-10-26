"""Initial DQ automation schema."""
from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table("datasets", sa.Column("id",sa.Integer(),primary_key=True),sa.Column("name",sa.String(200),nullable=False),sa.Column("filename",sa.String(255),nullable=False),sa.Column("path",sa.String(1000),nullable=False),sa.Column("source_type",sa.String(30),nullable=False),sa.Column("schema_json",sa.JSON(),nullable=False),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False))
    op.create_index("ix_datasets_name","datasets",["name"])
    op.create_table("jobs", sa.Column("id",sa.Integer(),primary_key=True),sa.Column("name",sa.String(200),nullable=False),sa.Column("description",sa.Text()),sa.Column("dataset_ids",sa.JSON(),nullable=False),sa.Column("selected_columns",sa.JSON(),nullable=False),sa.Column("join_config",sa.JSON()),sa.Column("destination",sa.JSON(),nullable=False),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False),sa.Column("updated_at",sa.DateTime(timezone=True),nullable=False))
    op.create_index("ix_jobs_name","jobs",["name"])
    op.create_table("credentials",sa.Column("id",sa.Integer(),primary_key=True),sa.Column("name",sa.String(200),nullable=False),sa.Column("provider",sa.String(50),nullable=False),sa.Column("secret_ciphertext",sa.Text(),nullable=False),sa.Column("config",sa.JSON(),nullable=False),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False))
    op.create_index("ix_credentials_provider","credentials",["provider"])
    op.create_table("executions",sa.Column("id",sa.Integer(),primary_key=True),sa.Column("job_id",sa.Integer(),sa.ForeignKey("jobs.id",ondelete="CASCADE"),nullable=False),sa.Column("status",sa.String(30),nullable=False),sa.Column("attempt",sa.Integer(),nullable=False),sa.Column("retry_of",sa.Integer(),sa.ForeignKey("executions.id")),sa.Column("passed_count",sa.Integer(),nullable=False),sa.Column("failed_count",sa.Integer(),nullable=False),sa.Column("report_path",sa.String(1000)),sa.Column("error",sa.Text()),sa.Column("logs",sa.JSON(),nullable=False),sa.Column("started_at",sa.DateTime(timezone=True)),sa.Column("finished_at",sa.DateTime(timezone=True)))
    op.create_index("ix_executions_job_id","executions",["job_id"]);op.create_index("ix_executions_status","executions",["status"])
    op.create_table("rules",sa.Column("id",sa.Integer(),primary_key=True),sa.Column("job_id",sa.Integer(),sa.ForeignKey("jobs.id",ondelete="CASCADE"),nullable=False),sa.Column("dataset_id",sa.Integer(),sa.ForeignKey("datasets.id")),sa.Column("column_name",sa.String(255),nullable=False),sa.Column("rule_type",sa.String(50),nullable=False),sa.Column("parameters",sa.JSON(),nullable=False),sa.Column("enabled",sa.Boolean(),nullable=False),sa.Column("approved",sa.Boolean(),nullable=False),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False))
    op.create_index("ix_rules_job_id","rules",["job_id"]);op.create_index("ix_rules_rule_type","rules",["rule_type"])
    op.create_table("corrections",sa.Column("id",sa.Integer(),primary_key=True),sa.Column("execution_id",sa.Integer(),sa.ForeignKey("executions.id",ondelete="CASCADE"),nullable=False),sa.Column("row_index",sa.Integer(),nullable=False),sa.Column("column_name",sa.String(255),nullable=False),sa.Column("old_value",sa.Text()),sa.Column("new_value",sa.Text()),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False))
    op.create_index("ix_corrections_execution_id","corrections",["execution_id"])

def downgrade():
    op.drop_table("corrections");op.drop_table("rules");op.drop_table("executions");op.drop_table("credentials");op.drop_table("jobs");op.drop_table("datasets")
