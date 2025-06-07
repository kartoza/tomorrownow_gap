import React from "react";
import {
    Box,
    Button,
    Container,
    Heading,
    Text,
    VStack,
    SimpleGrid,
    HStack
} from "@chakra-ui/react";
import { openInNewTab } from "@/utils/url";
import { services } from "./types";
import { APIDocsURL } from "@/utils/constants";


const AgroMetHubSection: React.FC = () => {
    return (
        <Box
            id="hub"
            py={20}
            bgImage="url('/static/images/bg_hub.webp')"
            bgSize="cover"
            bgPos="top"
            color="white"
            >
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                    <VStack gap={4} textAlign="center">
                        <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                        Agro-Met Intelligence Hub
                        </Heading>
                        <Text variant={"subTitle"}>
                        Trusted, localized insights for organizations on the front lines of helping farmers increase productivity and resilience.
                        </Text>
                    </VStack>
        
                    <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} maxW={{base: "full", md: "70%", lg: "85%"}}>
                        {services.map((service, index) => (
                        <Box key={index} bg="white"  p={7.5} borderRadius="lg" boxShadow="5px 5px 10px 0px rgba(16, 55, 92, 0.25)">
                            <VStack gap={4} align="start">
                                <HStack align="center" w="full" justify={"center"}>
                                    <Box w={'55px'} h={'55px'} bgSize={"contain"} bgPos="center" bgImage={`url('${service.icon}')`} />
                                </HStack>
                                <Heading as="h3" fontSize={'xl'} color="text.primary" alignItems={"center"} textAlign={"center"} w={"full"}>
                                    {service.title}
                                </Heading>
                                <VStack gap={2} align="start" w="full">
                                    {service.items.map((item, itemIndex) => (
                                    <HStack key={itemIndex} gap={2} align="start">
                                        <Box w={2} h={2} bg="brand.600" borderRadius="full" mt={2} flexShrink={0} />
                                        <Text fontSize="sm" color="text.primary" lineHeight={"alignBulletPoint"} textAlign={"start"}>
                                        {item}
                                        </Text>
                                    </HStack>
                                    ))}
                                </VStack>
                            </VStack>
                        </Box>
                        ))}
                    </SimpleGrid>
        
                    <VStack gap={4} textAlign="center">
                        <Button visual="solid" size="md" onClick={() => openInNewTab(APIDocsURL)}>
                        See API Docs
                        </Button>
                    </VStack>
                </VStack>
            </Container>
        </Box>
    );
};

export default AgroMetHubSection;