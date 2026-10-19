"""Freeze the exact rule lifecycle state used by each validation execution."""
from alembic import op
import sqlalchemy as sa

revision = "0002_execution_rule_snapshot"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("executions", sa.Column("rule_snapshot", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("executions", "rule_snapshot")
