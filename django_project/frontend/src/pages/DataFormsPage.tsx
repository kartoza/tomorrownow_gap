import React from "react"
import { Center, VStack, Text } from "@chakra-ui/react"
import MeasurementForm from "@/features/gap_api/MeasurementForm"
import CropPlanForm from "@/features/gap_api/CropPlanForm"
import LocationForm from "@/features/gap_api/LocationForm"

const DataFormsPage: React.FC = () => {
  return (
    <Center py={4} px={2}>
      <VStack gap={4} maxW="md" width="100%">
        <Text fontSize="2xl" fontWeight="bold" mb={4}>
          Data Forms
        </Text>
        <Text fontSize="md" color="black.600">
          Use these forms to submit data requests.
        </Text>
        <Text fontSize="sm" color="black.500">
          Fill out the forms below to request measurement data, crop plans, or
          upload location data. Each form will guide you through the necessary
          fields and options.
        </Text>
        <Text fontSize="sm" color="black.500">
          Ensure you have the required permissions to access the data you are
          requesting.
        </Text>
        <MeasurementForm />
        <CropPlanForm />
        <LocationForm />
      </VStack>
    </Center>
  )

}

export default DataFormsPage
