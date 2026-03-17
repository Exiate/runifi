# Implementation Plan: Issue #280

## Analysis

### Current State
- ProcessorConfigModal.tsx has 5 tabs (settings, scheduling, properties, relationships, comments)
- Scheduling strategy and interval are readOnly/disabled text inputs
- Processor name is readOnly/disabled
- Relationships tab only shows auto-terminate checkboxes (no retry config)
- Execution node selection is missing

### Backend Analysis
- `ProcessorConfigUpdateRequest` DTO already has `scheduling_strategy` and `scheduling_interval_ms` fields
- The API handler `update_processor_config` does NOT pass these fields to the engine
- The engine `update_processor_config` does NOT accept scheduling params
- `ProcessorInfo.scheduling` is a plain field, not Arc<RwLock<...>>
- `ProcessorNode.scheduling` is consumed at creation, used in `wait_for_trigger()`
- Making scheduling mutable requires: Arc<RwLock<SchedulingStrategy>> on ProcessorNode + ProcessorInfo, update in handle, update scheduling_display

### Scope Decision
Focus on what can be done cleanly:

**Phase 1 (this PR):**
1. Make scheduling strategy + interval editable in the UI (form state, dropdown, editable input)
2. Wire scheduling_strategy + scheduling_interval_ms through the API handler to the engine
3. Make ProcessorInfo.scheduling and scheduling_display mutable (Arc<RwLock<...>>)
4. Make ProcessorNode.scheduling an Arc<RwLock<...>> so wait_for_trigger reads updated value
5. Make processor name editable + add rename support (backend + UI)
6. Add execution node selection dropdown (UI + backend field on ProcessorInfo)
7. Add retry configuration display on relationships tab (UI only — placeholders for backend)

## Changes

### 1. Backend: Mutable scheduling on ProcessorInfo
**File**: `crates/runifi-core/src/engine/handle.rs`
- Change `ProcessorInfo.scheduling` from `SchedulingStrategy` to `Arc<RwLock<SchedulingStrategy>>`
- Change `ProcessorInfo.scheduling_display` from `String` to `Arc<RwLock<String>>`
- Update `update_processor_config` to accept and apply `scheduling_strategy` and `scheduling_interval_ms`
- Add `rename_processor` method

### 2. Backend: Mutable scheduling on ProcessorNode
**File**: `crates/runifi-core/src/engine/processor_node.rs`
- Change `self.scheduling` from `SchedulingStrategy` to `Arc<RwLock<SchedulingStrategy>>`
- Update `wait_for_trigger` to read from the lock
- Update constructor

### 3. Backend: Wire scheduling through FlowEngine
**File**: `crates/runifi-core/src/engine/flow_engine.rs`
- Pass Arc<RwLock<SchedulingStrategy>> when creating ProcessorNode
- Update scheduling_display assignment

### 4. Backend: Wire scheduling through MutationHandler
**File**: `crates/runifi-core/src/engine/mutation_handler.rs`
- Same pattern as flow_engine.rs for hot-added processors

### 5. Backend: Wire scheduling through API handler
**File**: `crates/runifi-api/src/routes/processors.rs`
- Pass `body.scheduling_strategy` and `body.scheduling_interval_ms` to engine

### 6. Backend: Update persistence
**File**: `crates/runifi-core/src/engine/persistence.rs`
- Update reads of scheduling_display to go through lock

### 7. Frontend: Scheduling Tab
**File**: `crates/runifi-api/dashboard-react/src/components/ProcessorConfigModal.tsx`
- Add `schedulingStrategy` and `schedulingIntervalMs` to ConfigFormState
- Initialize from config response
- Replace readOnly text input with dropdown for strategy
- Replace readOnly text input with editable number input for interval
- Include in save payload

### 8. Frontend: Processor Name Editing
- Add `name` to ConfigFormState
- Make name input editable
- Call rename API on save if name changed

### 9. Frontend: Execution Node Selection
- Add execution_node dropdown to scheduling tab
- Backed by a new field on ProcessorInfo

### 10. Frontend: Retry Configuration (UI only)
- Add retry attempt count, back off policy, max back off period columns to relationships table
- Currently read-only with placeholder values since backend doesn't support it yet

## Testing
- Unit tests for scheduling update in handle.rs
- Unit tests for rename
- Verify existing tests pass with Arc<RwLock<SchedulingStrategy>> changes
