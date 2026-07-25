# Migration from Hourly Loop to Dynamic Self-Learning System

## Overview

The old hourly loop system has been **completely replaced** with a new dynamic, event-driven AI assistant that features:

- **Event-driven monitoring** instead of periodic hourly analysis
- **Sparse operation** - triggers only on significant events (5-10 times per day vs every hour)
- **Self-learning capabilities** - AI learns from past experiences automatically
- **Advanced memory** - Semantic search with embeddings for contextual recall
- **Multi-model routing** - Uses appropriate models based on task complexity

## What Was Removed

### Constants & Globals
- `ASSISTANT_LOOP_PATH`
- `ASSISTANT_LOOP_CONFIG_PATH`
- `ASSISTANT_LOOP_MAX`
- `ASSISTANT_LOOP_ERROR_BACKOFF_SEC`
- `ASSISTANT_LOOP_THREAD`
- `ASSISTANT_LOOP_STOP`
- `ASSISTANT_LOOP_LOCK`

### Functions
- `_load_assistant_loop_state()`
- `_save_assistant_loop_state()`
- `_assistant_loop_defaults()`
- `_normalize_loop_config()`
- `_load_assistant_loop_config()`
- `_save_assistant_loop_config()`
- `_append_assistant_loop_summary()`
- `_latest_assistant_loop_summary()`
- `_assistant_loop_prompt()`
- `_assistant_loop_system_prompt()`
- `_assistant_loop_run_once()`
- `_assistant_loop_worker()`

### Startup/Shutdown
- `_start_assistant_loop_worker()` startup function
- `_stop_assistant_loop_worker()` shutdown function

### API Endpoints
- `GET /assistant/loop_state` - Get loop summaries
- `GET /assistant/loop_config` - Get loop configuration
- `POST /assistant/loop_config` - Update loop configuration
- `POST /assistant/loop_run` - Trigger loop run manually

### Context References
- Removed `assistant_loop` from context data
- Removed from system prompt documentation

## What Was Added

### New System Architecture

```
MonitorManager
├── EventProcessor (deduplication, rate limiting, priority queue)
├── StrategicAgent (AI coordinator with memory)
│   ├── MemoryManager (embeddings + vector search)
│   ├── ContextBuilder (portfolio + cryptids + signals)
│   └── ModelPipeline (Ollama integration + routing)
├── Monitors (background threads)
│   ├── PortfolioMonitor (5min, detects swings & concentration)
│   ├── SignalMonitor (2min, detects flips & consensus)
│   └── CryptidHealthMonitor (2min, detects crashes & errors)
└── LearningWorker (background thread)
    ├── Reflection (24h, extracts insights from events)
    ├── Pattern Discovery (6h, finds correlations & cascades)
    └── Prediction Evaluation (weekly, learns from accuracy)
```

### New Files Created

#### Core Infrastructure
- `app/assistant/__init__.py` - Module initialization
- `app/assistant/embeddings.py` - Vector embedding generation (sentence-transformers)
- `app/assistant/vector_store.py` - SQLite-based semantic search
- `app/assistant/memory.py` - High-level memory management API

#### Event System
- `app/assistant/events/event.py` - Event data structures
- `app/assistant/events/event_processor.py` - Deduplication & rate limiting

#### Monitors
- `app/assistant/monitors/base_monitor.py` - Base monitor class
- `app/assistant/monitors/portfolio_monitor.py` - Portfolio change detection
- `app/assistant/monitors/signal_monitor.py` - Cryptid signal monitoring
- `app/assistant/monitors/cryptid_health_monitor.py` - Health monitoring

#### AI Agent
- `app/assistant/strategic_agent.py` - Main AI coordinator
- `app/assistant/model_pipeline.py` - Ollama integration & model routing
- `app/assistant/prompts.py` - Prompt templates
- `app/assistant/context_builder.py` - Context aggregation

#### Self-Learning System
- `app/assistant/learning.py` - Reflection, pattern extraction, prediction evaluation
- `app/assistant/learning_worker.py` - Autonomous background learning thread
- `app/assistant/monitor_manager.py` - Coordinates all monitors and learning

#### Configuration
- `app/data/assistant_monitors_config.json` - Monitor settings

### New API Endpoints

- `GET /assistant/health` - Monitor system health stats
- `GET /assistant/events/recent?hours=24&limit=50` - Recent events
- `POST /assistant/analyze/ticker` - Ticker-specific analysis
- `POST /assistant/review/portfolio` - Comprehensive portfolio review
- `GET /assistant/memory/search?q=query&limit=10` - Semantic memory search
- `GET /assistant/memory/stats` - Memory system statistics

### Enhanced Endpoints

- `POST /assistant/chat` - Now uses new StrategicAgent with memory (fallback to legacy)

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

New dependencies:
- `sentence-transformers==3.3.1` - For embeddings
- `numpy==1.26.4` - For vector operations

### 2. Download Ollama Models

```bash
# Fast model for quick queries
ollama pull llama3.2:3b

# Balanced model (default)
ollama pull llama3.1:8b

# Strategic analysis
ollama pull qwen2.5:14b

# Deep analysis (optional)
ollama pull qwen2.5:32b
```

### 3. Configuration

The system is configured via `app/data/assistant_monitors_config.json`:

```json
{
  "global": {
    "ai_analysis_max_per_hour": 10,
    "ai_analysis_max_per_day": 50,
    "learning_enabled": true,
    "learning_reflection_hours": 24,
    "learning_pattern_hours": 6
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

Optional environment variables:

```bash
# Disable monitoring system
export ASSISTANT_MONITORS_ENABLED=0

# Ollama configuration (already in place)
export OLLAMA_URL=http://localhost:11434
export OLLAMA_MODEL=llama3.1:8b
```

## Key Differences

### Old Hourly Loop
- ⏰ Runs every hour (or configured interval)
- 📊 Compares current state to previous summaries
- 💾 Stores up to 12 summaries
- 🔄 Always active when enabled
- 📝 Simple text-based context
- ❌ No memory beyond 12 summaries
- ❌ No learning capabilities

### New Dynamic System
- ⚡ Event-driven (triggers on significant changes)
- 🎯 Sparse operation (5-10 triggers per day expected)
- 📈 Portfolio swings ≥5%, position swings ≥10%
- 🔔 Signal flips, consensus detection, health issues
- 🧠 Semantic memory with embeddings
- 📚 Self-learning from past experiences
- 🔄 Rate limited (10/hour, 50/day configurable)
- 🎨 Multi-model routing based on complexity
- 💡 Pattern discovery and prediction evaluation

## Expected Behavior

### Quiet by Default
The system is designed to be **quiet**. You won't see constant AI activity.

**Expected triggers per day:** 5-10
**Maximum triggers per hour:** 10 (rate limited)
**Maximum triggers per day:** 50 (rate limited)

### When AI Activates

**Portfolio Monitor (every 5 minutes checks):**
- Portfolio value changes ≥5%
- Single position changes ≥10%
- Concentration risk >25% in one position
- New positions opened or closed

**Signal Monitor (every 2 minutes checks):**
- Cryptid signal flips on held positions (BUY→SELL, SELL→BUY)
- Consensus detected (4+ cryptids agree on same ticker)
- Divergence on held positions (cryptids split 50%+)

**Cryptid Health Monitor (every 2 minutes checks):**
- Cryptid process crashes (CRITICAL - bypasses rate limit)
- Heartbeat timeout >5 minutes
- Error spike (10+ errors in 10 minutes)

**Learning Worker (autonomous background):**
- Reflection: Every 24 hours (reviews events, generates insights)
- Pattern Discovery: Every 6 hours (finds correlations, temporal patterns)
- Prediction Evaluation: Weekly (learns from accuracy)

### Event Deduplication

The system prevents spam by:
- **Cooldown periods:** 5min-1hour per event type + ticker
- **Similarity detection:** Won't trigger duplicate events
- **Priority queue:** Critical events bypass rate limits
- **Rate limiting:** Hard caps on AI invocations

## Migration Checklist

- [x] Remove old hourly loop constants and globals
- [x] Remove old hourly loop functions
- [x] Remove old startup/shutdown functions
- [x] Remove old API endpoints
- [x] Update system prompt (remove assistant_loop reference)
- [x] Update context builder (remove assistant_loop data)
- [x] Add new monitor manager startup/shutdown
- [x] Add new API endpoints
- [x] Update requirements.txt
- [x] Create configuration file
- [x] Implement self-learning system
- [x] Integrate learning worker into monitor manager

## Testing

### 1. Check System Health

```bash
curl http://localhost:8000/assistant/health
```

Expected response:
```json
{
  "status": "ok",
  "stats": {
    "monitors": {
      "portfolio_monitor": {"status": "running", "checks": 5, "events": 2},
      "signal_monitor": {"status": "running", "checks": 12, "events": 1},
      "cryptid_health_monitor": {"status": "running", "checks": 12, "events": 0}
    },
    "event_processor": {
      "total_events": 3,
      "analyses_triggered": 2,
      "budget_remaining": 8
    },
    "learning": {
      "status": "running",
      "last_reflection": "2026-03-05T10:30:00Z",
      "last_pattern_check": "2026-03-06T04:00:00Z"
    }
  }
}
```

### 2. Check Recent Events

```bash
curl http://localhost:8000/assistant/events/recent?hours=24&limit=10
```

### 3. Search Memory

```bash
curl "http://localhost:8000/assistant/memory/search?q=portfolio+risk&limit=5"
```

### 4. Check Memory Stats

```bash
curl http://localhost:8000/assistant/memory/stats
```

### 5. Test Chat (now uses new agent)

```bash
curl -X POST http://localhost:8000/assistant/chat \
  -H "Content-Type: application/json" \
  -d '{"prompt": "What is the current portfolio status?"}'
```

## Troubleshooting

### Dependencies Not Installed

**Error:** `ModuleNotFoundError: No module named 'sentence_transformers'`

**Solution:**
```bash
pip install sentence-transformers numpy
```

### Ollama Not Running

**Error:** Connection refused to `http://localhost:11434`

**Solution:**
```bash
ollama serve
```

### Ollama Model Not Found

**Error:** `Model 'llama3.1:8b' not found`

**Solution:**
```bash
ollama pull llama3.1:8b
```

### System Not Starting

Check logs on startup. The system will print:
- `[AssistantMonitors] Started successfully` - Good!
- `[AssistantMonitors] Dependencies not installed: ...` - Install dependencies
- `[AssistantMonitors] Failed to start: ...` - Check error message

### Too Many Triggers

If the AI is activating too frequently, adjust thresholds in `assistant_monitors_config.json`:

```json
{
  "global": {
    "ai_analysis_max_per_hour": 5,  // Reduce from 10
    "ai_analysis_max_per_day": 25   // Reduce from 50
  },
  "portfolio_monitor": {
    "triggers": {
      "portfolio_swing_pct": 10.0,    // Increase from 5.0
      "position_swing_pct": 20.0      // Increase from 10.0
    }
  }
}
```

### Not Enough Triggers

If you want more frequent analysis:

```json
{
  "portfolio_monitor": {
    "triggers": {
      "portfolio_swing_pct": 2.0,     // Decrease from 5.0
      "position_swing_pct": 5.0       // Decrease from 10.0
    }
  }
}
```

## Benefits of New System

### 1. **Intelligent Triggering**
- Only runs when something significant happens
- Respects your attention - won't spam you

### 2. **Memory & Learning**
- Remembers past events and conversations
- Learns patterns automatically
- Improves predictions over time
- Semantic search finds relevant context

### 3. **Better Context Awareness**
- Knows about portfolio state
- Tracks cryptid signals in real-time
- Monitors operational health
- Understands correlations and patterns

### 4. **Resource Efficient**
- Uses appropriate model for task complexity
- Rate limited to prevent runaway costs
- Sparse operation conserves compute

### 5. **Extensible**
- Easy to add new monitors
- Pluggable event types
- Configurable thresholds
- Modular architecture

## Future Enhancements

Potential additions (not yet implemented):

- **News Monitor:** Trigger on earnings, SEC filings, news sentiment
- **Market Regime Monitor:** Detect VIX spikes, sector rotation, correlation breakdown
- **Indicator Monitor:** Advanced technical pattern detection
- **External Data Monitor:** Economic indicators, Fed announcements
- **Anomaly Detection:** Statistical outliers in price/volume
- **Prediction Tracking:** Explicitly track AI predictions and measure accuracy
- **Multi-agent Debate:** Multiple models discuss and reach consensus
- **Tool Use:** Allow AI to query external APIs (financial data, news)

## Support

For issues or questions:
1. Check the logs on startup
2. Review `AI_ASSISTANT_README.md` for detailed documentation
3. Check `IMPLEMENTATION_SUMMARY.md` for technical details
4. Verify configuration in `assistant_monitors_config.json`

## Conclusion

The migration from hourly loop to dynamic self-learning system is **complete**.

The old system has been **completely removed** from the codebase. All functionality is now provided by the new event-driven architecture with self-learning capabilities.

**Key Takeaway:** The AI is now **intelligent, sparse, and self-improving** rather than **periodic and static**.
