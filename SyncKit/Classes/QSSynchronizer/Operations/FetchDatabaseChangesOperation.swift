//
//  FetchDatabaseChangesOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 18/05/2018.
//

import Foundation
import CloudKit

class FetchDatabaseChangesOperation: CloudKitSynchronizerOperation {
    
    let database: CloudKitDatabaseAdapter
    let databaseToken: CKServerChangeToken?
    let completion: (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()
    
    var changedZoneIDs = [CKRecordZone.ID]()
    var deletedZoneIDs = [CKRecordZone.ID]()
    weak var internalOperation: CKFetchDatabaseChangesOperation?
    
    init(database: CloudKitDatabaseAdapter, databaseToken: CKServerChangeToken?, completion: @escaping (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()) {
        self.databaseToken = databaseToken
        self.database = database
        self.completion = completion
        super.init()
    }
    
    override func start() {
        super.start()

        let databaseChangesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseToken)
        databaseChangesOperation.fetchAllChanges = true

        databaseChangesOperation.recordZoneWithIDChangedBlock = { zoneID in
            self.changedZoneIDs.append(zoneID)
            debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> recordZoneWithIDChangedBlock")

        }

        databaseChangesOperation.recordZoneWithIDWasDeletedBlock = { zoneID in
            self.deletedZoneIDs.append(zoneID)
            debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> recordZoneWithIDWasDeletedBlock")

        }

        databaseChangesOperation.fetchDatabaseChangesCompletionBlock = { serverChangeToken, moreComing, operationError in
            debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> moreComing = \(moreComing)")

            if !moreComing {
                
                if operationError == nil {
                    
                    self.completion(serverChangeToken, self.changedZoneIDs, self.deletedZoneIDs)
                    debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> completion")
                }

                self.finish(error: operationError)
                
                
                debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> finish(error: \(operationError))")
            }
            
            
        }
        databaseChangesOperation.queuePriority = .veryHigh
        databaseChangesOperation.qualityOfService = .userInteractive
        internalOperation = databaseChangesOperation
        database.add(databaseChangesOperation)
        debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> add operation")

    }
    
    override func cancel() {
        debugPrint("QSCloudKitSynchronizer >> FetchDatabaseChangesOperation >> cancel")

        internalOperation?.cancel()
        super.cancel()
    }
}
