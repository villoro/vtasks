# vtasks.backups subflow

The `vtasks.backups` subflow is responsible for creating dated backups of important files, such as KeePass vaults. It selectively backs up files that have undergone changes to avoid wasting resources.

For example, let's consider the following file:

```plaintext
- KeePass
  └── vault.kdbx
```

Over time, the subflow generates backups as follows:

```plaintext
- KeePass
  ├── Backups
  │   ├── 2023
  │   │   ├── 2023_06_12 vault.kdbx
  │   │   └── 2023_06_01 vault.kdbx
  │   └── 2022
  │       ├── 2022_12_01 vault.kdbx
  │       └── 2023_11_05 vault.kdbx
  └── vault.kdbx
```

To optimize storage usage, the subflow removes some of the backups from the previous month, considering that older backups become less useful over time.
