import React from "react";
import {
    Box,
    Container,
    Heading,
    Text,
    VStack,
    Grid,
    GridItem,
    Image
} from "@chakra-ui/react";


const GLobalImpactSection: React.FC = () => {
    return (
        <Box id="impact" py={20}>
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                <VStack gap={4} textAlign="center">
                    <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                    Global Impact
                    </Heading>
                    <Text variant={"subTitle"}>
                    Our north star vision is 100 Million Weather-Resilient Farmers Across Africa.
                    </Text>
                </VStack>
    
                <Grid templateColumns={{ base: '1fr', lg: '1fr 1fr' }} gap={12} alignItems="center">
                    <GridItem alignSelf={"stretch"} display={"flex"}>
                    <Image
                        src="/static/images/map_africa.webp"
                        alt="Africa map showing global impact"
                        w="full"
                        borderRadius="lg"
                    />
                    </GridItem>
                    <GridItem>
                    <VStack gap={8} align="stretch">
                        <Box w="full">
                        <Heading as="h3" size="md" fontWeight={"extrabold"} textAlign={"center"}>
                            What Drives Our Work?
                        </Heading>
                        </Box>
                        <Box bg="white" p={7.5} borderRadius="2lg" boxShadow="5px 5px 10px 2px rgba(56, 69, 60, 0.25)" >
                        <VStack gap={4} align="start">
                            <Heading as="h3" fontSize={"xl"} color="brand.600" fontWeight={"bold"}>
                                The Problem
                            </Heading>
                            <Text color="text.primary" textAlign={"start"} fontStyle={"italic"}>
                                Limited access to actionable weather data
                            </Text>
                            <Text color="text.primary" textAlign={"start"}>
                                Smallholder farmers face increasing weather uncertainty with limited
                                access to reliable, localized weather information and value they need to make
                                informed decisions about when to plant, irrigate, and harvest their crops.
                            </Text>
                        </VStack>
                        </Box>
                        
                        <Box bg="brand.100" p={7.5} borderRadius="2lg" boxShadow="5px 5px 10px 2px rgba(56, 69, 60, 0.25)">
                        <VStack gap={4} align="start">
                            <Heading as="h3" fontSize={"xl"} color="brand.600" fontWeight={"bold"}>
                                Our Solution
                            </Heading>
                            <Text color="text.primary" textAlign={"start"} fontStyle={"italic"}>
                                Agro-Met intelligence - localized, accessible, affordable
                            </Text>
                            <Text color="text.primary" textAlign={"start"}>
                                GAP bridges the first-mile technology with last-mile impact by making advanced weather products accessible and useful for organizations supporting smallholder farmers through more open data, localized products, and  community partnerships.
                            </Text>
                        </VStack>
                        </Box>
                    </VStack>
                    </GridItem>
                </Grid>
                </VStack>
            </Container>
        </Box>
    );
};

export default GLobalImpactSection;