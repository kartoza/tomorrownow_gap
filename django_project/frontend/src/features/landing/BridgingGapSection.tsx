import React from "react";
import { Box, Container, Heading, Text, VStack } from "@chakra-ui/react";
import { CropPlanImage } from "./CropPlanImage";

const BridgingGapSection: React.FC = () => {
    return (
        <Box id="bridging" py={20}>
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                    One Platform. Every Weather-Resilient Decision.
                </Heading>
                <Text variant={"subTitle"}>
                    From localized Agro-Met data to trusted insights, help your farmers grow more weather resilient and increase productivity.
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