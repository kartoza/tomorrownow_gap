import React from "react";
import { Box, Container, Heading, Text, VStack } from "@chakra-ui/react";
import { CropPlanImage } from "./CropPlanImage";

const BridgingGapSection: React.FC = () => {
    return (
        <Box id="bridging" py={20}>
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                    Bridging The Gap
                </Heading>
                <Text variant={"subTitle"}>
                    Agro-Met intelligence is the integration of weather, climate, and agronomic data with advanced analytics to provide actionable insights that optimise farming, boost productivity, and manage agricultural risks.
                </Text>
                <Box w="full">
                    {CropPlanImage()}
                </Box>
                </VStack>
            </Container>
        </Box>
    );
};

export default BridgingGapSection;