# vtasks.archive subflow

The `vtasks.archive` subflow is responsible for archiving files in Dropbox by renaming them and moving them to subfolders based on the year. It also extracts specific data, such as payrolls, from zipped files.

For example, let's consider the following files:

```plaintext
- docs
  ├── 2023
  │   └── 2023_02 payroll.pdf
  ├── 2022
  │   └── 2022_06_24 fight BCN-CAG.pdf
  ├── 2023_03_payroll.pdf
  ├── 2023_01_12 fight BCN-RAK.pdf
  └── 2022_12_payroll.pdf
```

After the `vtasks.archive` subflow is applied, the files will be organized as follows:

```plaintext
- docs
  ├── 2023
  │   ├── 2023_03 payroll.pdf
  │   ├── 2023_01_12 fight BCN-RAK.pdf
  │   └── 2023_02 payroll.pdf
  └── 2022
      ├── 2022_12 payroll.pdf
      └── 2022_06_24 fight BCN-CAG.pdf
```

This subflow ensures that the files are appropriately archived by moving them into the respective year subfolders while preserving the original file names.
