// -------------------------------------------------------------------
//
// hot_backup.h
//
// Copyright (c) 2011-2016 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#ifndef STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_
#define STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_

namespace leveldb
{

// Called every 60 seconds to test for external hot backup trigger
//   (initiates backup if trigger seen)
void CheckHotBackupTrigger();

} // namespace leveldb


#endif  // STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_
