const datasetId = 'specDataset';
const testData = { test: 'text' };
const updateData = { test: 'something else' };

describe('Sync', function() {

  beforeAll(function(done) {
    initDataset('specDataset', { syncFrequency: 1 }, 10, done);
  });

  beforeEach(function() {
    $fh.sync.init({ sync_frequency: 1, storage_strategy: 'dom' , crashed_count_wait: 1});
  });

  afterEach(function(done) {
    $fh.sync.stopSync(datasetId, done, done.fail);
  });

  afterAll(function() {
    return removeDataset(datasetId)();
  });

  it('should manage a dataset', function() {
    $fh.sync.manage(datasetId);
    return waitForSyncEvent('sync_complete')();
  });

  it('should list', function() {
    return manage(datasetId)
    .then(waitForSyncEvent('sync_started'))
    .then(function verifySyncStarted(event) {
      expect(event.dataset_id).toEqual(datasetId);
      expect(event.message).toBeNull();
    })
    .then(waitForSyncEvent('sync_complete'))
    .then(function verifySyncCompleted(event) {
      expect(event.dataset_id).toEqual(datasetId);
      expect(event.message).toEqual('online');
    });
  });

  it('should create', function() {
    // set up a notifier that only handles `local_update_applied' events as these might
    // occur before the 'then' part of the following promise being called.
    $fh.sync.notify(function(event) {
      if (event.code === 'local_update_applied') {
        expect(event.dataset_id).toEqual(datasetId);
        expect(event.message).toMatch(/(load|create)/);
      }
    });
    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function(res) {
      expect(res.action).toEqual('create');
      expect(res.post).toEqual(testData);
    })
    .then(waitForSyncEvent('remote_update_applied'))
    .then(function verifyUpdateApplied(event) {
      expect(event.dataset_id).toEqual(datasetId);
      expect(event.message.type).toEqual('applied');
      expect(event.message.action).toEqual('create');
    });
  });

  it('should read', function() {
    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function withResult(res) {
      const uid = res.uid;
      return doRead(datasetId, uid)
      .then(function verifyData(data) {
        expect(data.data).toEqual(testData);
        expect(data.hash).not.toBeNull();
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should fail when reading unknown uid', function() {
    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function withResult() {
      return doRead(datasetId, 'bogus_uid');
    })
    .catch(function verifyError(err) {
      expect(err).toEqual('unknown_uid');
    });
  });

  it('should update', function() {
    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function withResult(res) {
      const uid = res.uid;
      return doUpdate(datasetId, uid, updateData)
      .then(doRead(datasetId, uid))
      .then(function verifyUpdate(data) {
        expect(data).toEqual(updateData);
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should delete', function() {
    return manage(datasetId)
      .then(doCreate(datasetId, testData))
      .then(function withResult(res) {
        const uid = res.uid;
        return doDelete(datasetId, uid)
        .then(doRead(datasetId, uid).catch(function(err) {
          expect(err).toEqual('unknown_uid');
        }));
      });
  });

  it('should cause a collision', function() {
    const collisionData = { test: 'cause a collision' };

    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(waitForSyncEvent('remote_update_applied'))
    .then(function verifyUpdateApplied(event) {
      // We need to store this for updating in MongoDB in the next step.
      expect(event.message.uid).not.toBeNull();
      // The UID of the record which should have a collision.
      var recordId = event.message.uid;
      return updateRecord(datasetId, recordId, collisionData)
      .then(doUpdate(datasetId, recordId , updateData))
      .then(waitForSyncEvent('collision_detected'))
      .then(function verifyCorrectCollision(event) {
        // Assert that the collision is the one we caused.
        expect(event.message.uid).toEqual(recordId);
      })
      .then(listCollisions(datasetId))
      .then(function verifyCollisionInList(collisions) {
        // Find the collision we invoked earlier.
        const invokedCollision = searchObject(collisions, function(collision) {
          return collision.uid === recordId;
        });
        // Assert that the collision is the one we caused.
        expect(invokedCollision).not.toBeNull();
        return invokedCollision;
      })
      .then(removeCollision)
      .then(listCollisions(datasetId))
      .then(function verifyNoCollisions(collisions) {
        // There should be no collisions left. We deleted the only one.
        expect(collisions).toEqual({});
      })
      .catch(function(err) {
        expect(err).toBeNull();
      });
    });
  });

  it('should create records created by other clients', function() {
    const recordToCreate = { test: 'create' };

    return manage(datasetId)
    .then(createRecord(datasetId, recordToCreate))
    .then(waitForSyncEvent('record_delta_received'))
    .then(function verifyDeltaStructure(event) {
      expect(event.uid).not.toBeNull();
      expect(event.message).toEqual('create');
      expect(event.dataset_id).toEqual(datasetId);
      return doRead(datasetId, event.uid)
      .then(function verifyCorrectRecordApplied(record) {
        expect(record.data).toEqual(recordToCreate);
      })
      .catch(function(err) {
        expect(err).toBeNull();
      });
    });
  });

  it('should update records updated by other clients', function() {
    const updateData = { test: 'cause a client update' };

    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(waitForSyncEvent('remote_update_applied'))
    .then(function verifyUpdateApplied(event) {
      const uid = event.uid;
      return updateRecord(datasetId, uid, updateData)
      .then(waitForSyncEvent('record_delta_received'))
      .then(function verifyDeltaStructure(event) {
        expect(event.message).toEqual('update');
        return doRead(datasetId, event.uid);
      })
      .then(function verifyRecordUpdated(record) {
        expect(record.data).toEqual(updateData);
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should manage multiple datasets', function() {
    const datasetOneId = 'specDatasetOne';
    const datasetTwoId = 'specDatasetTwo';

    const recordOne = { test: 'recordOne' };
    const recordTwo = { test: 'recordTwo' };
    // We will use these to get the record from `doList` later.
    var recordOneHash;
    var recordTwoHash;

    return manage(datasetOneId)
    .then(manage(datasetTwoId))
    .then(doCreate(datasetOneId, recordOne))
    .then(waitForSyncEvent('remote_update_applied'))
    .then(function setRecordTwoHash(event) {
      expect(event.uid).not.toBeNull();
      recordOneHash = event.uid;
    })
    .then(doCreate(datasetTwoId, recordTwo))
    .then(waitForSyncEvent('remote_update_applied'))
    .then(function setRecordTwoHash(event) {
      expect(event.uid).not.toBeNull();
      recordTwoHash = event.uid;
    })
    .then(doList(datasetOneId))
    .then(function verifyDatasetOneUpdates(records) {
      expect(records[recordTwoHash]).not.toBeDefined();
      expect(records[recordOneHash]).not.toBeNull();
      expect(records[recordOneHash].data).toEqual(recordOne);
    })
    .then(doList(datasetTwoId))
    .then(function verifyDatasetTwoUpdates(records) {
      expect(records[recordOneHash]).not.toBeDefined();
      expect(records[recordTwoHash]).not.toBeNull();
      expect(records[recordTwoHash].data).toEqual(recordTwo);
    })
    .then(removeDataset(datasetOneId))
    .then(removeDataset(datasetTwoId))
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should stop remote updates on stopSync', function() {
    // `local_update_applied` might be sent before `doCreate` finishes.
    $fh.sync.notify(function(event) {
      if (event.code === 'local_update_applied') {
        expect(event.dataset_id).toEqual(datasetId);
        expect(event.message).toMatch(/(load|create)/);
      }
    });

    return manage(datasetId)
    .then(stopSync(datasetId))
    .then(doCreate(datasetId, testData))
    .then(function(record) {
      // Wait time to ensure `remote_update_applied` is called after online.
      return waitForSyncEvent('sync_complete')()
      .then(startSync(datasetId))
      .then(waitForSyncEvent('remote_update_applied'))
      .then(function verifyCorrectRecord(event) {
        const recordUid = $fh.sync.getUID(record.hash);
        expect(event.uid).toEqual(recordUid);
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should not stop remote updates using forceSync while sync not active', function() {
    // `local_update_applied` might be sent before `doCreate` finishes.
    $fh.sync.notify(function(event) {
      if (event.code === 'local_update_applied') {
        expect(event.dataset_id).toEqual(datasetId);
        expect(event.message).toMatch(/(load|create)/);
      }
    });

    return manage(datasetId)
    .then(stopSync(datasetId))
    .then(doCreate(datasetId, testData))
    .then(function(record) {
      // Wait time to ensure `remote_update_applied` is called after online.
      return waitForSyncEvent('sync_complete')()
      .then(forceSync(datasetId))
      .then(waitForSyncEvent('remote_update_applied'))
      .then(function verifyCorrectRecord(event) {
        const recordUid = $fh.sync.getUID(record.hash);
        expect(event.uid).toEqual(recordUid);
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should sync after client goes offline', function() {
    $fh.sync.notify(function(event) {
      if (event.code === 'offline_update') {
        expect(event.dataset_id).toEqual(datasetId);
        expect(event.message).toEqual('update');
      }
    });

    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function(res) {
      const uid = res.uid;
      return doUpdate(datasetId, uid, updateData)
        .then(doRead(datasetId, uid))
        .then(function verifyUpdate(data) {
          expect(data).toEqual(updateData);
        })
        .then(offline())
        .then(doUpdate(datasetId, uid, updateData))
        .then(online())
        .then(doRead(datasetId, uid))
        .then(waitForSyncEvent('remote_update_applied'));
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should handle crashed records', function() {
    return setServerStatus({ crashed: true })()
    .then(manage(datasetId, { sync_frequency: 2 }))
    .then(doCreate(datasetId, testData))
    .then(function(record) {
      // Wait twice to ensure record was included in pending at the time.
      return waitForSyncEvent('sync_failed')()
      .then(waitForSyncEvent('sync_failed'))
      .then(getPending(datasetId))
      .then(function verifyPendingRecordCrashed(pending) {
        expect(pending[record.hash].inFlight).toBe(true);
        expect(pending[record.hash].crashed).toBe(true);
      })
      .then(setServerStatus({ crashed: false }))
      .then(waitForSyncEvent('remote_update_applied'))
      .then(function verifyCorrectRecordApplied(event) {
        // A record has been applied, check that its our record.
        const recordUid = $fh.sync.getUID(record.hash);
        expect(event.uid).toEqual(recordUid);
      })
      .then(getPending(datasetId))
      .then(function verifyNoPending(pending) {
        expect(pending).toEqual({});
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should remove dataset when clearCache is called', function() {
    return manage(datasetId)
      .then(doCreate(datasetId, testData))
      .then(function withResult(res) {
        const uid = res.uid;
        return clearCache(datasetId)()
          .then(doRead(datasetId, uid)
          .catch(function(err) {
            expect(err).toEqual('unknown_dataset ' + datasetId);
          })
          .then(manage(datasetId))); // this is to keep afterEach from failing
      });
  });

  it('should update uid after remote update', function() {
    return manage(datasetId)
    .then(doCreate(datasetId, testData))
    .then(function(record) {
      return new Promise(function verifyUidIsHash(resolve) {
        const recordUid = $fh.sync.getUID(record.hash);
        expect(record.hash).toEqual(recordUid);
        resolve();
      })
      .then(waitForSyncEvent('remote_update_applied'))
      .then(function verifyUidIsUpdated(event) {
        const recordUid = $fh.sync.getUID(record.hash);
        expect(event.uid).toEqual(recordUid);
      });
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  });

  it('should produce dataset delta when scaled', function() {
    const scaleDataset = 'specDataset_scaling';

    return manage(scaleDataset)
    .then(doCreate(scaleDataset, { test: 'scaled' }))
    .then(waitForSyncEvent('sync_complete'))
    .then(doList(scaleDataset))
    .then(function(records) {
      expect(records).toBeDefined();
      console.log('list', JSON.stringify(records));
      return waitForSyncEvent('sync_complete')()
      .then(scaleServer('scaleUp', 7))
      .then(waitForSyncEvent('delta_received'))
      .then(waitForSyncEvent('sync_failed'))
      .then(waitForSyncEvent('sync_failed'))
      .then(waitForSyncEvent('syfdsafa'));
    })
    .catch(function(err) {
      expect(err).toBeNull();
    });
  }, 60000);
});

function offline() {
  return setOnline(false);
}

function online() {
  return setOnline(true);
}

function setOnline(online) {
  return new Promise(function(resolve) {
    navigator.network = navigator.network || {};
    navigator.network.connection = navigator.network.connection || {};
    navigator.network.connection.type = online ? 'WiFi' : 'none';
    resolve(online);
  });
}

function manage(dataset, options) {
  return new Promise(function(resolve) {
    $fh.sync.manage(dataset, options, {}, {}, function() {
      resolve();
    });
  });
}

function clearCache(datasetId) {
  return function() {
    return new Promise(function(resolve) {
      $fh.sync.clearCache(datasetId, function() {
        return resolve('cleared');
      });
    });
  };
}

function doCreate(dataset, data) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.doCreate(dataset, data, function(res) {
        resolve(res);
      }, function(err) {
        reject(err);
      });
    });
  };
}

function doDelete(datasetId, uid) {
  return new Promise(function(resolve, reject) {
    $fh.sync.doDelete(datasetId, uid, function() {
      resolve(true);
    }, function failure(err) {
      reject(err);
    });
  });
}

function doRead(datasetId, uid) {
  return new Promise(function(resolve, reject) {
    $fh.sync.doRead(datasetId, uid, function(data) {
      resolve(data);
    }, function failure(err) {
      reject(err);
    });
  });
}

function doList(dataset) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.doList(dataset, function(res) {
        resolve(res);
      }, reject);
    });
  };
}

function doUpdate(datasetId, uid, updateData) {
  return new Promise(function(resolve, reject) {
    $fh.sync.doUpdate(datasetId, uid, updateData, function() {
      resolve(updateData);
    }, function(err) {
      reject(err);
    });
  });
}

function listCollisions(datasetId) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.listCollisions(datasetId, function(collisions) {
        expect(collisions).not.toBeNull();
        resolve(collisions);
      }, function(err) {
        reject(err);
      });
    });
  };
}

function removeCollision(collision) {
  return new Promise(function(resolve, reject) {
    $fh.sync.removeCollision(datasetId, collision.hash, resolve, reject);
  });
}

function getPending(dataset) {
  return function() {
    return new Promise(function(resolve) {
      $fh.sync.getPending(dataset, resolve);
    });
  };
}

/**
 * Update the value of a record. Used to cause a collision.
 */
function updateRecord(dataset, uid, record) {
  return new Promise(function(resolve, reject) {

    const updatePath = '/datasets/' + dataset + '/records/' + uid;
    const recordData = { data: record };

    $fh.cloud({
      path: updatePath,
      data: recordData,
      method: 'put'
    }, function() {
      resolve({ uid: uid });
    }, reject);
  });
}

/**
 * Create a record in the database, avoiding sync.
 *
 * @param {Object} record - The record to create.
 */
function createRecord(dataset, record) {
  return function() {
    return new Promise(function(resolve, reject) {
      const createPath = '/datasets/' + dataset + '/records';
      const recordData = { data: record };

      $fh.cloud({
        path: createPath,
        data: recordData
      }, resolve, reject);
    });
  };
}

/**
 * Empty all collections in MongoDB which correspond to a particular dataset.
 *
 * @param {string} dataset - The name of the dataset to remove collections for.
 */
function removeDataset(dataset) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.cloud({ path: '/datasets/' + dataset + '/reset' }, resolve, reject);
    });
  };
}

/**
 * Start sync for a specified dataset.
 *
 * @param {string} dataset - The dataset to start syncing.
 */
function startSync(dataset) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.startSync(dataset, resolve, reject);
    });
  };
}

/**
 * Force sync for a particular dataset. Even if sync is inactive.
 *
 * @param {string} dataset - The name of the dataset to force a sync on.
 */
function forceSync(dataset) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.forceSync(dataset, resolve, reject);
    });
  };
}

/**
 * Stop sync for a specified dataset. Only local updates will be applied.
 *
 * @param {string} dataset - The dataset to stop syncing.
 */
function stopSync(dataset) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.sync.stopSync(dataset, resolve, reject);
    });
  };
}

/**
 * Wait for a specific notification to be made from the client SDK.
 *
 * @param {string} expectedEvent - The name of the event to wait for.
 */
function waitForSyncEvent(expectedEvent) {
  return function() {
    return new Promise(function(resolve) {
      $fh.sync.notify(function(event) {
        console.log('event', event.code);
        if (event.code === 'record_delta_received' || event.code === 'delta_received') {
          console.log(JSON.stringify(event));
        }
        if (event.code === expectedEvent) {
          expect(event.code).toEqual(expectedEvent); // keep jasmine happy with at least 1 expectation
          resolve(event);
        }
      });
    });
  };
}

/**
 * Set the server into or out of a crashed state with a custom response status.
 *
 * @param {Object} status - The status object.
 * @param {boolean} status.crashed - If the server should act as crashed.
 * @param {number} status.crashStatus - If crashed, what status code to use.
 */
function setServerStatus(status) {
  const serverStatus = { status: status };
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.cloud({
        path: '/server/status',
        data: serverStatus
      }, resolve, reject);
    });
  };
}

/**
 * Tell the server to increase or decrease the amount of workers it has in its
 * cluster.
 *
 * @param {('scaleUp'|'scaleDown')} command - Command to invoke on the server.
 * @param {number} - The number of workers to add or remove fromt he cluster.
 */
function scaleServer(command, amount) {
  return function() {
    return new Promise(function(resolve, reject) {
      $fh.cloud({
        path: '/server/' + command,
        data: {
          amount: amount
        }
      }, resolve, reject);
    });
  };
}

/**
 * Performed at the start of the tests. Try to initialise the dataset on the
 * server side through the added `POST /datasets` endpoint. Try at most ten
 * times and fail if it cannot be initialised in this time.
 *
 * @param {string} dataset - The name of the dataset to initialise.
 * @param {Object} options - Options to use on the dataset, server-side.
 * @param {number} retries - The amount of times to try to create the dataset,
 *                           with one second intervals.
 * @param {Function} done - The done callback function provided by Jasmine.
 */
function initDataset(dataset, options, retries, done) {
  $fh.cloud({
    path: '/datasets',
    data: {
      name: dataset,
      options: options
    }
  }, done, function() {
    const remainingRetries = retries - 1;
    if (remainingRetries > 0) {
      setTimeout(function() {
        return initDataset(dataset, options, remainingRetries, done);
      }, 1000);
    } else {
      return done.fail();
    }
  });
}

/**
 * Iterate through the elements of an object and return the first element
 * which returns `true` for the `test` function argument.
 *
 * @param {Object} obj - The object to search.
 * @param {Function} test - The function to test each element with.
 * @returns {any} - The first element in `obj` which passed `test`.
 */
function searchObject(obj, test) {
  for (var key in obj) {
    if (obj.hasOwnProperty(key) && test(obj[key])) {
      return obj[key];
    }
  }
}

