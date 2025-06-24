import React from "react";
import { useSelector, useDispatch } from 'react-redux';
import {
  Box,
  VStack,
  Text,
  Card,
  Button,
  Input,
  Field,
  Textarea,
  ButtonGroup,
  Badge,
  Separator as Divider,
} from '@chakra-ui/react';
import { RootState } from '@/app/store';
import {
  updateFormData,
  resetForm,
  setDrawingMode,
  clearBoundingBox,
} from '@/features/map/mapSlice';
import { FormData } from '@/features/map/types';

export const SearchForm: React.FC = () => {
  const dispatch = useDispatch();
  const { boundingBox, isDrawingMode, formData } = useSelector(
    (state: RootState) => state.map
  );

  const handleInputChange = (field: keyof FormData, value: string) => {
    dispatch(updateFormData({ [field]: value }));
  };

  const handleStartDrawing = () => {
    dispatch(setDrawingMode(true));
  };

  const handleClearBoundingBox = () => {
    dispatch(clearBoundingBox());
  };

  const handleSearch = () => {
    console.log('Search with:', { formData, boundingBox });
    // Implement search logic here
  };

  const handleReset = () => {
    dispatch(resetForm());
  };

  return (
    <Box w="350px" p={4} bg="white" boxShadow="md" overflowY="auto">
      <VStack gap={4} align="stretch">
        <Card.Root>
          <Card.Header>
            <Text fontSize="lg" fontWeight="bold">
              Data Browser
            </Text>
          </Card.Header>
          <Card.Body>
            <VStack gap={3}>
              <Divider />

              {/* Area Selection */}
              <Box w="full">
                <Text fontSize="sm" fontWeight="medium" mb={2}>
                  Area Selection
                </Text>
                <VStack gap={2}>
                  <Button
                    colorScheme="blue"
                    variant={isDrawingMode ? "solid" : "outline"}
                    size="sm"
                    w="full"
                    onClick={handleStartDrawing}
                    disabled={isDrawingMode}
                  >
                    {isDrawingMode ? "Drawing Mode Active" : "Draw Rectangle on Map"}
                  </Button>
                  
                  {isDrawingMode && (
                    <Badge colorScheme="blue" variant="subtle">
                      Click two corners on the map to create rectangle
                    </Badge>
                  )}
                </VStack>
              </Box>

              <ButtonGroup gap={2}>
                <Button size={"sm"} visual={"solid"} flex={1} onClick={handleSearch}>
                  Search
                </Button>
                <Button size={"sm"} variant="outline" flex={1} onClick={handleReset}>
                  Reset
                </Button>
              </ButtonGroup>
            </VStack>
          </Card.Body>
        </Card.Root>

        {/* Bounding Box Display */}
        {boundingBox && (
          <Card.Root>
            <Card.Header>
              <Text fontSize="lg" fontWeight="bold" color="green.600">
                Selected Area ✓
              </Text>
            </Card.Header>
            <Card.Body>
              <VStack gap={2} align="stretch" fontSize="sm">
                <Text><strong>North:</strong> {boundingBox.north.toFixed(6)}</Text>
                <Text><strong>South:</strong> {boundingBox.south.toFixed(6)}</Text>
                <Text><strong>East:</strong> {boundingBox.east.toFixed(6)}</Text>
                <Text><strong>West:</strong> {boundingBox.west.toFixed(6)}</Text>
                <Button size="sm" colorScheme="red" onClick={handleClearBoundingBox}>
                  Clear Selection
                </Button>
              </VStack>
            </Card.Body>
          </Card.Root>
        )}
      </VStack>
    </Box>
  );
};