/*
 * Copyright 2014 Jason Woods.
 *
 * This file is a modification of code from Logstash Forwarder.
 * Copyright 2012-2013 Jordan Sissel and contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
  "encoding/json"
  "github.com/op/go-logging"
  "os"
)

func (e *NewFileEvent) Process(state map[*ProspectorInfo]*FileState) {
  log.Debug("Registrar received a new file event for %s", e.Source)

  // A new file we need to save offset information for so we can resume
  state[e.ProspectorInfo] = &FileState{
    Source: &e.Source,
    Offset: e.Offset,
  }
  state[e.ProspectorInfo].PopulateFileIds(e.fileinfo)
}

func (e *DeletedEvent) Process(state map[*ProspectorInfo]*FileState) {
  if log.IsEnabledFor(logging.DEBUG) {
    if _, ok := state[e.ProspectorInfo]; ok {
      log.Debug("Registrar received a deletion event for %s", *state[e.ProspectorInfo].Source)
    } else {
      log.Warning("Registrar received a deletion event for UNKNOWN (%p)", e.ProspectorInfo)
    }
  }

  // Purge the registrar entry - means the file is deleted so we can't resume
  // This keeps the state clean so it doesn't build up after thousands of log files
  delete(state, e.ProspectorInfo)
}

func (e *RenamedEvent) Process(state map[*ProspectorInfo]*FileState) {
  _, is_found := state[e.ProspectorInfo]
  if !is_found {
    // This is probably stdin or a deleted file we can't resume
    return
  }

  log.Debug("Registrar received a rename event for %s -> %s", state[e.ProspectorInfo].Source, e.Source)

  // Update the stored file name
  state[e.ProspectorInfo].Source = &e.Source
}

func (e *EventsEvent) Process(state map[*ProspectorInfo]*FileState) {
  if len(e.Events) == 1 {
    log.Debug("Registrar received offsets for %d log entries", len(e.Events))
  } else {
    log.Debug("Registrar received offsets for %d log entries", len(e.Events))
  }

  for _, event := range e.Events {
    _, is_found := state[event.ProspectorInfo]
    if !is_found {
      // This is probably stdin then or a deleted file we can't resume
      continue
    }

    state[event.ProspectorInfo].Offset = event.Offset
  }
}

type Registrar struct {
  control        *LogCourierControl
  registrar_chan chan []RegistrarEvent
  references     int
  persistdir     string
  statefile      string
  state          map[*ProspectorInfo]*FileState
}

func NewRegistrar(persistdir string, control *LogCourierMasterControl) *Registrar {
  return &Registrar{
    control: control.Register(),
    registrar_chan: make(chan []RegistrarEvent, 16),
    persistdir: persistdir,
    statefile: ".log-courier",
    state: make(map[*ProspectorInfo]*FileState),
  }
}

func (r *Registrar) LoadPrevious() map[string]*ProspectorInfo {
  // Generate ProspectorInfo structures for registrar and prosector to communicate with
  data := make(map[string]*FileState)

  // Load the previous state
  filename := r.persistdir + string(os.PathSeparator) + ".log-courier"
  f, err := os.Open(filename)
  if err != nil {
    // Try the .new file - maybe we failed mid-move
    filename = r.persistdir + string(os.PathSeparator) + ".log-courier.new"
    f, err = os.Open(filename)
  }

  if err != nil {
    // Failed to load previous state, return nil
    return nil
  }

  // Parse the data
  log.Notice("Loaded registrar data from %s", filename)

  decoder := json.NewDecoder(f)
  decoder.Decode(&data)
  f.Close()

  r.state = make(map[*ProspectorInfo]*FileState, len(data))
  resume := make(map[string]*ProspectorInfo, len(data))

  for file, state := range data {
    resume[file] = NewProspectorInfoFromFileState(file, state)
    r.state[resume[file]] = state
  }

  return resume
}

func (r *Registrar) Connect() chan<- []RegistrarEvent {
  // TODO: Is there a better way to do this?
  r.references++
  return r.registrar_chan
}

func (r *Registrar) Disconnect() {
  r.references--
  if r.references == 0 {
    // Shutdown registrar, all references are closed
    close(r.registrar_chan)
  }
}

func (r *Registrar) Register() {
  defer func() {
    r.control.Done()
  }()

  // Ignore shutdown channel - wait for registrar to close
  for events := range r.registrar_chan {
    for _, event := range events {
      event.Process(r.state)
    }

    state_json := make(map[string]*FileState, len(r.state))
    for _, value := range r.state {
      //if _, ok := state_json[*value.Source]; ok {
        // Panic? We should never allow this
      //}
      state_json[*value.Source] = value
    }

    r.WriteRegistry(state_json)
  }

  log.Info("Registrar shutdown complete")
}
