"""Persist execution stage and progress for polling clients."""
from alembic import op
import sqlalchemy as sa

revision = "0007_execution_progress"
down_revision = "0006_workflow_selected_columns"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("executions", sa.Column("stage", sa.String(40), nullable=False, server_default="queued"))
    op.add_column("executions", sa.Column("progress_completed", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("executions", sa.Column("progress_total", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("executions", sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_column("executions", "updated_at")
    op.drop_column("executions", "progress_total")
    op.drop_column("executions", "progress_completed")
    op.drop_column("executions", "stage")
