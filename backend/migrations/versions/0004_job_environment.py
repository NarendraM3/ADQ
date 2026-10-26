"""Persist the platform selected for each job."""
from alembic import op
import sqlalchemy as sa

revision = "0004_job_environment"
down_revision = "0003_job_settings"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "environment" not in columns:
        # Keep legacy values NULL: existing records do not identify their platform reliably.
        op.add_column("jobs", sa.Column("environment", sa.String(length=100), nullable=True))


def downgrade():
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "environment" in columns:
        op.drop_column("jobs", "environment")
