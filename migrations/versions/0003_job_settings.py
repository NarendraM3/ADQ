"""Bring the original jobs table in line with the current model."""
from alembic import op
import sqlalchemy as sa

revision = "0003_job_settings"
down_revision = "0002_execution_rule_snapshot"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("jobs")}
    if "settings" not in columns:
        op.add_column("jobs", sa.Column("settings", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))


def downgrade():
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("jobs")}
    if "settings" in columns:
        op.drop_column("jobs", "settings")
