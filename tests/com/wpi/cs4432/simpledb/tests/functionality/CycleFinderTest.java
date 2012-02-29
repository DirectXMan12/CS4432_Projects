package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import simpledb.tx.concurrency.graph.ElementaryCyclesSearch;
import simpledb.tx.concurrency.graph.GraphNode;

public class CycleFinderTest
{
	
	@Test
	public void testNoCycles()
	{
		boolean[][] adjMatrix = new boolean[31][31];
		for (int i = 0; i < 31; i++) for (int j = 0; j < 31; j++) adjMatrix[i][j] = false;
		GraphNode[] nodes = new GraphNode[31];
		
		int ind = 1;
		nodes[0] = new GraphNode(0);
		for (int i = 0; i < 5; i++)
		{
			nodes[ind] = new GraphNode((i+1)*10);
			adjMatrix[0][ind] = true;
			int rInd = ind;
			for (int j = 0; j < 5; j++)
			{
				ind++;
				nodes[ind] = new GraphNode((i+1)*10+j+1);
				adjMatrix[rInd][ind] = true;
			}
		}
		
		ElementaryCyclesSearch ecs = new ElementaryCyclesSearch(adjMatrix, nodes);
		List cycles = ecs.getElementaryCycles();
		
		assertEquals(0, cycles.size());
	}
	
	@Test
	public void testOneCycle()
	{
		boolean[][] adjMatrix = new boolean[31][31];
		for (int i = 0; i < 31; i++) for (int j = 0; j < 31; j++) adjMatrix[i][j] = false;
		GraphNode[] nodes = new GraphNode[31];
		
		int ind = 0;
		nodes[0] = new GraphNode(0);
		for (int i = 0; i < 5; i++)
		{
			ind++;
			nodes[ind] = new GraphNode((i+1)*10);
			adjMatrix[0][ind] = true;
			int rInd = ind;
			for (int j = 0; j < 5; j++)
			{
				ind++;
				nodes[ind] = new GraphNode((i+1)*10+j+1);
				adjMatrix[rInd][ind] = true;
			}
		}
		
		adjMatrix[30][0] = true;
		
		ElementaryCyclesSearch ecs = new ElementaryCyclesSearch(adjMatrix, nodes);
		List cycles = ecs.getElementaryCycles();
		
		assertEquals(1, cycles.size());
	}
}
