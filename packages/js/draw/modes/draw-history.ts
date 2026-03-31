/**
 * Vertex-level undo/redo for active drawing.
 * Tracks vertices as they're added. Undo removes last vertex, redo re-adds it.
 */

export class DrawHistory {
  private _undoStack: [number, number][] = [];
  private _redoStack: [number, number][] = [];
  private _removeLastVertex: (() => void) | null = null;
  private _addVertex: ((coord: [number, number]) => void) | null = null;

  private static _activeInstance: DrawHistory | null = null;

  static get active(): DrawHistory | null {
    return DrawHistory._activeInstance;
  }

  get hasUndo(): boolean {
    return this._undoStack.length > 0;
  }

  get hasRedo(): boolean {
    return this._redoStack.length > 0;
  }

  setVertexCallbacks(
    removeLastVertex: () => void,
    addVertex: (coord: [number, number]) => void
  ): void {
    this._removeLastVertex = removeLastVertex;
    this._addVertex = addVertex;
  }

  pushVertex(coord: [number, number]): void {
    this._undoStack.push([...coord]);
    this._redoStack = [];
  }

  undoVertex(): boolean {
    if (this._undoStack.length === 0) return false;
    const vertex = this._undoStack.pop()!;
    this._redoStack.push(vertex);
    this._removeLastVertex?.();
    return true;
  }

  redoVertex(): boolean {
    if (this._redoStack.length === 0) return false;
    const vertex = this._redoStack.pop()!;
    this._undoStack.push(vertex);
    this._addVertex?.(vertex);
    return true;
  }

  clear(): void {
    this._undoStack = [];
    this._redoStack = [];
  }

  activate(): void {
    DrawHistory._activeInstance = this;
  }

  deactivate(): void {
    if (DrawHistory._activeInstance === this) {
      DrawHistory._activeInstance = null;
    }
  }
}
