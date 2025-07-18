import React from "react";
import { Box, Container, Heading, Text, VStack, SimpleGrid, Image } from "@chakra-ui/react";
import { openInNewTab } from "@/utils/url";
import { aboutPartners } from "./types";


const AboutUsSection: React.FC = () => {
    return (
        <Box
            id="about"
            py={20}
            bgImage="url('/static/images/about_us.webp')"
            bgSize="cover"
            bgPos="top"
            >
            <Container px={{ base: 4, md: 6 }} w="full">
                <VStack gap={12}>
                <VStack gap={4} textAlign="center">
                    <Heading as="h2" size={{ base: "md", md: "lg" }} variant={"default"}>
                    About Us
                    </Heading>
                    <Text variant={"subTitle"}>
                    TomorrowNow is an accelerator transforming Africa's small-scale farming landscape through Digital Agromet Intelligence. 
                    We bridge the gap between cutting-edge science and on-farm decisions - enabling access to quality weather and climate data and enabling actionable agronomic advisories through strategic partnerships, digital platforms, and product innovation.
                    </Text>
                </VStack>
    
                <SimpleGrid columns={{ base: 1, md: 3 }} gap={{ base: 6, md: 8 }} w="full">
                    {aboutPartners.map((partner, index) => (
                    <Box
                        key={index}
                        textAlign="center"
                        alignSelf={"center"}
                        onClick={() => openInNewTab(partner.website, partner.name)}
                        _hover={{ 
                        opacity: 1, 
                        transform: 'scale(1.05)',
                        cursor: 'pointer'
                        }}
                        transition="all 0.3s ease"
                    >
                        <Image src={partner.logo} alt={partner.name} mx="auto" mb={6} objectFit={"contain"} />
                    </Box>
                    ))}
                </SimpleGrid>
                </VStack>
            </Container>
        </Box>
    );
};

export default AboutUsSection;