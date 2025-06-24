import React from "react"
import MeasurementForm from "@/features/gap_api/MeasurementForm"
import CropPlanForm from "@/features/gap_api/CropPlanForm"
import LocationForm from "@/features/gap_api/LocationForm"

const DataFormsPage: React.FC = () => {
  return (
    <>
      <MeasurementForm />
      <CropPlanForm />
      <LocationForm />
    </>
  )
}

export default DataFormsPage
