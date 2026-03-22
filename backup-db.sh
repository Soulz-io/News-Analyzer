#!/bin/bash
# Daily backup of News Analyzer SQLite database
BACKUP_DIR="/home/opposite/openclaw-news-analyzer/engine/data/backups"
DB_PATH="/home/opposite/openclaw-news-analyzer/engine/data/news_analyzer.db"
MAX_BACKUPS=7  # Keep 7 days of backups

mkdir -p "$BACKUP_DIR"

if [ -f "$DB_PATH" ]; then
    BACKUP_FILE="$BACKUP_DIR/news_analyzer_$(date +%Y%m%d_%H%M%S).db"
    sqlite3 "$DB_PATH" ".backup '$BACKUP_FILE'"
    
    if [ $? -eq 0 ]; then
        # Compress the backup
        gzip "$BACKUP_FILE"
        echo "$(date): Backup created: ${BACKUP_FILE}.gz"
        
        # Remove old backups (keep last MAX_BACKUPS)
        ls -t "$BACKUP_DIR"/news_analyzer_*.db.gz 2>/dev/null | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm
        echo "$(date): Old backups cleaned (keeping last $MAX_BACKUPS)"
    else
        echo "$(date): ERROR: Backup failed!"
        exit 1
    fi
else
    echo "$(date): ERROR: Database not found at $DB_PATH"
    exit 1
fi
