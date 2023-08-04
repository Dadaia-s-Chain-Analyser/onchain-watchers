// SPDX-License-Identifier: MIT
pragma solidity ^0.6.0;

interface AggregatorInterface {

  function description() external view returns (string memory);
  function decimals() external view returns (uint8);
  function latestRound() external view returns (uint256);
  function latestTimestamp() external view returns (uint256);
  function getTimestamp(uint256 _roundId) external view returns (uint256);


  function getRoundData(uint80 _roundId) external view returns (
      uint80 roundId,
      int256 answer,
      uint256 startedAt,
      uint256 updatedAt,
      uint80 answeredInRound
    );

  function latestRoundData() external view returns (
      uint80 roundId,
      int256 answer,
      uint256 startedAt,
      uint256 updatedAt,
      uint80 answeredInRound
    );
}