# ZENKO PRIME - Enhanced AI Assistant System

## Overview

The enhanced AI assistant system for Dreadfox Trader provides autonomous monitoring, context-aware memory, and strategic analysis using local Ollama models.

## Features

### 🧠 **Context Memory with Embeddings**
- Semantic search across historical conversations, events, and analyses
- Uses `sentence-transformers` (all-MiniLM-L6-v2) for local embeddings
- SQLite-based vector storage for fast similarity search
- Automatic context retrieval based on query relevance

### 🔍 **Autonomous Monitoring System**
- **Portfolio Monitor**: Detects significant value swings, concentration risks, position changes
- **Signal Monitor**: Tracks cryptid signal changes, consensus, and divergence
- **Cryptid Health Monitor**: Identifies crashed processes, heartbeat timeouts, error spikes

### 🤖 **Intelligent AI Agent (ZENKO PRIME)**
- Multi-model routing (fast → balanced → strategic → deep)
- Comprehensive context building (portfolio, cryptids, indicators, memory)
- Event-driven analysis with automatic triggering
- Strategic portfolio reviews and ticker explanations

### ⚡ **Event-Driven Architecture**
- Events only trigger on significant thresholds
- Smart deduplication and rate limiting
- Priority queue processing
- Configurable cooldowns per event type

## Installation

### 1. Install Dependencies

```bash
cd /path/to/DreadFoxTrader.1.0
pip install -r requirements.txt
```

This will install:
- `sentence-transformers==3.3.1` (for embeddings)
- `numpy==1.26.4` (for vector operations)

### 2. Install Ollama Models

```bash
# Default local model
ollama pull gpt-oss:latest

# Optional tier overrides can be configured with:
# OLLAMA_MODEL_FAST, OLLAMA_MODEL_BALANCED, OLLAMA_MODEL_STRATEGIC, OLLAMA_MODEL_DEEP
```

### 3. Configure Monitors

Edit `app/data/assistant_monitors_config.json`:

```json
{
  "global": {
    "ai_analysis_max_per_hour": 10,
    "ai_analysis_max_per_day": 50
  },
  "portfolio_monitor": {
    "enabled": true,
    "interval_sec": 300,
    "triggers": {
      "portfolio_swing_pct": 5.0,
      "position_swing_pct": 10.0,
      "concentration_threshold_pct": 25.0
    }
  },
  "signal_monitor": {
    "enabled": true,
    "interval_sec": 120,
    "triggers": {
      "consensus_threshold": 4,
      "divergence_threshold_pct": 50
    }
  },
  "cryptid_health_monitor": {
    "enabled": true,
    "interval_sec": 120,
    "triggers": {
      "heartbeat_timeout_sec": 300,
      "error_threshold": 10
    }
  }
}
```

### 4. Environment Variables

Add to your `.env` file:

```bash
# Ollama configuration
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=gpt-oss:latest

# Monitor system (optional, defaults to enabled)
ASSISTANT_MONITORS_ENABLED=1

# Legacy assistant loop (can disable if using new system)
ASSISTANT_LOOP_BACKGROUND=0
```

## Usage

### Starting the System

```bash
# Start the application
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Monitors will start automatically on startup
# Look for these log messages:
# [MonitorManager] Initialized PortfolioMonitor
# [MonitorManager] Initialized SignalMonitor
# [MonitorManager] Initialized CryptidHealthMonitor
# [MonitorManager] Started 3 monitors
```

### API Endpoints

#### **Chat with AI Assistant**
```bash
POST /assistant/chat
{
  "prompt": "What's happening with my portfolio?",
  "include_portfolio": true,
  "include_runs": true,
  "include_indicators": false
}
```

Response:
```json
{
  "reply": "Your portfolio is up 2.3% today...",
  "model": "gpt-oss:latest",
  "duration_sec": 3.2,
  "context_summary": {
    "num_positions": 15,
    "num_cryptids": 3,
    "memory_items": 5
  }
}
```

#### **Analyze Specific Ticker**
```bash
POST /assistant/analyze/ticker
{
  "ticker": "AAPL"
}
```

#### **Portfolio Review**
```bash
POST /assistant/review/portfolio
```

#### **Search Memory**
```bash
GET /assistant/memory/search?q=portfolio+drop&limit=10
```

#### **Monitor Health**
```bash
GET /assistant/health
```

Response:
```json
{
  "status": "ok",
  "stats": {
    "monitors": [
      {
        "name": "portfolio",
        "enabled": true,
        "interval_sec": 300,
        "health": "healthy",
        "events_generated": 12
      }
    ],
    "event_processor": {
      "queue_size": 0,
      "analysis_budget_remaining": 8
    },
    "agent": {
      "memory_stats": {
        "total_items": 145,
        "conversations": 87,
        "events": 42,
        "analyses": 16
      }
    }
  }
}
```

#### **Recent Events**
```bash
GET /assistant/events/recent?hours=24&limit=50
```

## Architecture

### Data Flow

```
External Sources (Portfolio, Cryptids, Market)
              ↓
      Context Builder
              ↓
    Monitors (Portfolio, Signal, Health)
              ↓
    Event Detection & Classification
              ↓
   Event Processor (Deduplication, Rate Limiting)
              ↓
    Strategic Agent (if threshold met)
              ↓
   Memory Storage (Embeddings)
              ↓
        User Interface
```

### File Structure

```
app/
├── assistant/
│   ├── __init__.py
│   ├── embeddings.py              # Embedding generation
│   ├── vector_store.py            # SQLite vector storage
│   ├── memory.py                  # Memory management
│   ├── context_builder.py         # Context aggregation
│   ├── prompts.py                 # Prompt templates
│   ├── model_pipeline.py          # Ollama API integration
│   ├── strategic_agent.py         # Main AI agent
│   ├── monitor_manager.py         # Monitor coordinator
│   ├── events/
│   │   ├── event.py               # Event data structures
│   │   └── event_processor.py     # Event processing pipeline
│   └── monitors/
│       ├── base_monitor.py        # Base monitor class
│       ├── portfolio_monitor.py   # Portfolio monitoring
│       ├── signal_monitor.py      # Signal monitoring
│       └── cryptid_health_monitor.py  # Health monitoring
├── data/
│   ├── assistant_memory/          # Vector embeddings cache
│   ├── assistant_monitors_config.json  # Monitor configuration
│   └── cryptid_exchange.sqlite3   # Database (includes new tables)
```

### Database Schema

New tables created automatically:

```sql
-- Vector memory storage
CREATE TABLE assistant_memory_vectors (
    id INTEGER PRIMARY KEY,
    ts INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    content_text TEXT NOT NULL,
    metadata_json TEXT,
    embedding_blob BLOB NOT NULL
);

-- Event storage
CREATE TABLE assistant_events (
    id INTEGER PRIMARY KEY,
    ts INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    tickers TEXT,
    data_json TEXT,
    context_json TEXT,
    ai_analysis_id INTEGER
);

-- AI analysis storage
CREATE TABLE assistant_analyses (
    id INTEGER PRIMARY KEY,
    ts INTEGER NOT NULL,
    event_id INTEGER,
    model_used TEXT,
    prompt_type TEXT,
    analysis_text TEXT,
    reasoning_text TEXT,
    recommendations_json TEXT
);
```

## Configuration Guide

### Monitor Intervals

- **Portfolio Monitor**: 300s (5 min) - Checks for significant portfolio changes
- **Signal Monitor**: 120s (2 min) - Monitors cryptid signal changes
- **Cryptid Health**: 120s (2 min) - Detects operational issues

### Trigger Thresholds

#### Portfolio
- `portfolio_swing_pct`: 5.0 - Trigger when total portfolio changes by 5%
- `position_swing_pct`: 10.0 - Trigger when single position changes by 10%
- `concentration_threshold_pct`: 25.0 - Warn when position exceeds 25% of portfolio

#### Signals
- `consensus_threshold`: 4 - Trigger when 4+ cryptids agree
- `divergence_threshold_pct`: 50 - Trigger when 50%+ cryptids disagree on held position

#### Cryptid Health
- `heartbeat_timeout_sec`: 300 - Trigger if no heartbeat for 5 minutes
- `error_threshold`: 10 - Trigger if 10+ errors in window

### Rate Limiting

- **Max analyses per hour**: 10 (configurable)
- **Max analyses per day**: 50 (configurable)
- **Critical events**: Always bypass limits

### Event Cooldowns

Prevents duplicate analyses:
- Portfolio events: 5 minutes
- Signal consensus: 10 minutes
- Cryptid crashes: No cooldown (always analyze)

## Expected Behavior

### Typical Trading Day

**Morning (Market Open)**
- No AI activation unless significant overnight changes

**During Trading Hours**
- 5-10 AI analyses on volatile days
- 1-3 analyses on quiet days

**Triggers:**
- Portfolio swings > 5%
- Signal consensus (4+ cryptids agree)
- Cryptid crashes (immediate)
- Signal flips on held positions

### Example Event Flow

1. **Portfolio drops 6%** → Portfolio Monitor detects
2. **Event created** → Severity: WARNING, Priority: HIGH
3. **Deduplication check** → Event is new, passes through
4. **Rate limit check** → 8/10 budget remaining, approved
5. **AI Analysis triggered** → Model: configured local Ollama model
6. **Analysis stored** → Embedded in memory for future retrieval
7. **User notified** → Via `/assistant/events/recent`

## Troubleshooting

### Monitors Not Starting

```bash
# Check logs
tail -f logs/app.log

# Verify dependencies
python -c "import sentence_transformers; print('OK')"

# Check Ollama
curl http://localhost:11434/api/tags
```

### Memory Issues

```bash
# Check memory stats
curl http://localhost:8000/assistant/memory/stats

# Clear old memory (keep last 90 days)
# Run in Python:
from pathlib import Path
from app.assistant.memory import MemoryManager
memory = MemoryManager(Path("app/data/cryptid_exchange.sqlite3"), Path("app/data/assistant_memory"))
memory.cleanup_old(days=90)
```

### High CPU Usage

The first time embeddings are generated, the model download and initialization may use CPU.
After initial setup, embedding generation is fast (<100ms per text).

### Disable Monitors Temporarily

```bash
# Via environment variable
export ASSISTANT_MONITORS_ENABLED=0

# Or edit config
# Set "enabled": false for specific monitors
```

## Performance Tips

1. **Model Selection**: Use `OLLAMA_MODEL_FAST`, `OLLAMA_MODEL_BALANCED`, `OLLAMA_MODEL_STRATEGIC`, or `OLLAMA_MODEL_DEEP` only if you intentionally install separate tier models.
2. **Indicator Loading**: Keep `include_indicators: false` unless specifically needed
3. **Memory Cleanup**: Run `memory.cleanup_old(90)` monthly to keep database lean
4. **Monitor Intervals**: Increase intervals if too many events (e.g., 600s instead of 300s)

## Roadmap

### Implemented ✅
- Context memory with embeddings
- Autonomous monitoring (portfolio, signals, health)
- Event-driven AI triggers
- Multi-model routing
- Strategic agent integration

### Future Enhancements 🚀
- Indicator breakout monitor
- Market regime monitor
- External data monitor (news, earnings)
- Scheduled summaries (morning, midday, EOD)
- Web UI for event feed
- Proactive alert system

## Support

For issues or questions:
1. Check logs: `tail -f logs/app.log`
2. Verify health: `GET /assistant/health`
3. Review events: `GET /assistant/events/recent`

## License

Part of Dreadfox Trader project.
