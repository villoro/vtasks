# vtasks.archive subflow

This subflow helps in archiving files in Dropbox by renaming them and moving them to subfolders based on the year.
It also extracts some data (such as payrolls) from zipped files.

For example let's imagine the following files:

```plaintext
- docs
  ├── 2023
  │   └── 2023_02 payroll.pdf
  ├── 2022
  │   └── 2022_06_24 fight BCN-CAG.pdf
  ├── 2023_03_payroll.pdf
  ├── 2023_01_12 fight BCN-RAK.pdf
  ├── 2022_12_payroll.pdf
  └── ...
```

It will archive them as following:

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