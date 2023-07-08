# vtasks.backups subflow

The `vtasks.backups` subflow is responsible for creating dated backups of important files, such as a KeePass vaults. In order to not waste resources it will only backup files that had changes.

For example, let's consider the following file:

```plaintext
- KeePass
  └── vault.kdbx
```

After some time using it, we would have the following backups:

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

Since old backups are less useful, it also removes some of the backups for the last month.
