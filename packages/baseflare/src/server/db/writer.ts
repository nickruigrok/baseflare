import type { DatabaseReader } from "./reader";

export interface DatabaseWriter<TDocument = Record<string, unknown>>
  extends DatabaseReader<TDocument> {
  delete(table: string, id: string): Promise<void>;
  insert(table: string, doc: Record<string, unknown>): Promise<string>;
  patch(
    table: string,
    id: string,
    partial: Record<string, unknown>
  ): Promise<void>;
  replace(
    table: string,
    id: string,
    doc: Record<string, unknown>
  ): Promise<void>;
}
