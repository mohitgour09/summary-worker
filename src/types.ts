export const encounterTypes = ['Appointment', 'MedicationRefill', 'TelephoneTriage'] as const;
export type EncounterType = typeof encounterTypes[number];

export interface EncounterEvent {
  event_id: string;
  encounter_id: string;
  patient_id: string;
  encounter_type: EncounterType;
  version: number;
  payload: { transcription: string };
}
