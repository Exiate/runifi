/** Alignment and distribution utilities for canvas nodes. */

interface NodeRect {
  id: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

const DEFAULT_WIDTH = 220;
const DEFAULT_HEIGHT = 100;

function toRect(node: { id: string; position: { x: number; y: number }; measured?: { width?: number; height?: number } }): NodeRect {
  return {
    id: node.id,
    x: node.position.x,
    y: node.position.y,
    width: node.measured?.width ?? DEFAULT_WIDTH,
    height: node.measured?.height ?? DEFAULT_HEIGHT,
  };
}

export type AlignableNode = { id: string; position: { x: number; y: number }; measured?: { width?: number; height?: number } };

/** Returns map of nodeId -> new position for horizontal left alignment. */
export function alignLeft(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const minX = Math.min(...rects.map((r) => r.x));
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: minX, y: r.y });
  }
  return result;
}

/** Align horizontal centers. */
export function alignCenter(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const centers = rects.map((r) => r.x + r.width / 2);
  const avgCenter = centers.reduce((a, b) => a + b, 0) / centers.length;
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: avgCenter - r.width / 2, y: r.y });
  }
  return result;
}

/** Align right edges. */
export function alignRight(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const maxRight = Math.max(...rects.map((r) => r.x + r.width));
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: maxRight - r.width, y: r.y });
  }
  return result;
}

/** Align top edges. */
export function alignTop(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const minY = Math.min(...rects.map((r) => r.y));
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: r.x, y: minY });
  }
  return result;
}

/** Align vertical middles. */
export function alignMiddle(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const middles = rects.map((r) => r.y + r.height / 2);
  const avgMiddle = middles.reduce((a, b) => a + b, 0) / middles.length;
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: r.x, y: avgMiddle - r.height / 2 });
  }
  return result;
}

/** Align bottom edges. */
export function alignBottom(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  const rects = nodes.map(toRect);
  const maxBottom = Math.max(...rects.map((r) => r.y + r.height));
  const result = new Map<string, { x: number; y: number }>();
  for (const r of rects) {
    result.set(r.id, { x: r.x, y: maxBottom - r.height });
  }
  return result;
}

/** Distribute nodes evenly left-to-right (horizontal spacing). */
export function distributeHorizontally(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  if (nodes.length < 3) return new Map(nodes.map((n) => [n.id, { ...n.position }]));
  const rects = nodes.map(toRect).sort((a, b) => a.x - b.x);
  const totalWidth = rects.reduce((s, r) => s + r.width, 0);
  const minX = rects[0].x;
  const maxRight = rects[rects.length - 1].x + rects[rects.length - 1].width;
  const totalSpace = maxRight - minX - totalWidth;
  const gap = totalSpace / (rects.length - 1);

  const result = new Map<string, { x: number; y: number }>();
  let currentX = minX;
  for (const r of rects) {
    result.set(r.id, { x: currentX, y: r.y });
    currentX += r.width + gap;
  }
  return result;
}

/** Distribute nodes evenly top-to-bottom (vertical spacing). */
export function distributeVertically(nodes: AlignableNode[]): Map<string, { x: number; y: number }> {
  if (nodes.length < 3) return new Map(nodes.map((n) => [n.id, { ...n.position }]));
  const rects = nodes.map(toRect).sort((a, b) => a.y - b.y);
  const totalHeight = rects.reduce((s, r) => s + r.height, 0);
  const minY = rects[0].y;
  const maxBottom = rects[rects.length - 1].y + rects[rects.length - 1].height;
  const totalSpace = maxBottom - minY - totalHeight;
  const gap = totalSpace / (rects.length - 1);

  const result = new Map<string, { x: number; y: number }>();
  let currentY = minY;
  for (const r of rects) {
    result.set(r.id, { x: r.x, y: currentY });
    currentY += r.height + gap;
  }
  return result;
}
