package com.mycompany.rocksdb.model;

import com.mycompany.rocksdb.enums.NfsStat3;

public class WRITE3res extends AbstractNfsResponse<WRITE3resok, WRITE3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public WRITE3res(NfsStat3 status, WRITE3resok resok, WRITE3resfail resfail) {
    super(status, resok, resfail);
  }

  public static WRITE3res createOk(WRITE3resok okData) {
    return new WRITE3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static WRITE3res createFail(NfsStat3 failStatus, WRITE3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new WRITE3res(failStatus, null, failData);
  }

}
