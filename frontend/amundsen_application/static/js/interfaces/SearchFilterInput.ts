// Copyright Contributors to the Amundsen project.
// SPDX-License-Identifier: Apache-2.0

export interface SearchFilterInput {
  categoryId: string;
  value: string | FilterOptions | undefined;
}

export type FilterOptions = { [id: string]: boolean };
