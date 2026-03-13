import { memo, useState, useCallback, useRef, useEffect } from 'react';
import { type Node, type NodeProps } from '@xyflow/react';
import type { LabelNodeData } from '../types/flow';

export type LabelNodeType = Node<LabelNodeData, 'labelNode'>;

function LabelNodeInner({ data }: NodeProps<LabelNodeType>) {
  const { labelId, text, width, height, backgroundColor, fontSize, pending } = data;
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState(text);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (editing && textareaRef.current) {
      textareaRef.current.focus();
      textareaRef.current.select();
    }
  }, [editing]);

  const handleDoubleClick = useCallback(() => {
    setEditText(text);
    setEditing(true);
  }, [text]);

  const handleSave = useCallback(() => {
    setEditing(false);
    const trimmed = editText.trim();
    if (trimmed === text) return;

    // Persist the text change to the backend
    fetch(`/api/v1/process-groups/root/labels/${encodeURIComponent(labelId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: trimmed }),
    }).catch(() => {});
  }, [editText, text, labelId]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Escape') {
        setEditing(false);
        setEditText(text);
      }
      // Shift+Enter for newlines, Enter alone to save
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSave();
      }
    },
    [text, handleSave],
  );

  const bgColor = backgroundColor || 'rgba(255, 255, 200, 0.12)';
  const fSize = fontSize || 14;

  return (
    <div
      className={`label-node${pending ? ' label-node-pending' : ''}`}
      style={{
        width: `${width || 200}px`,
        minHeight: `${height || 60}px`,
        backgroundColor: bgColor,
        fontSize: `${fSize}px`,
      }}
      onDoubleClick={handleDoubleClick}
      role="article"
      aria-label={`Label: ${text || 'empty'}${pending ? ' (pending)' : ''}`}
    >
      {editing ? (
        <textarea
          ref={textareaRef}
          className="label-node-editor"
          value={editText}
          onChange={(e) => setEditText(e.target.value)}
          onBlur={handleSave}
          onKeyDown={handleKeyDown}
          style={{ fontSize: `${fSize}px` }}
        />
      ) : (
        <div className="label-node-text">
          {text || 'Double-click to edit'}
        </div>
      )}
    </div>
  );
}

export const LabelNode = memo(LabelNodeInner);
