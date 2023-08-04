// SPDX-License-Identifier: agpl-3.0
pragma solidity 0.6.12;

/************
@title IPriceOracle interface
@notice Interface for the Aave price oracle.*/
interface IAaveOracleV2 {
  /***********
    @dev returns the asset price in ETH
     */
  function getAssetPrice(address asset) external view returns (uint256);

  function getAssetsPrices(address[] calldata assets) external view returns (uint256[] memory);


  /***********
    @dev sets the asset price, in wei
     */
  function setAssetPrice(address asset, uint256 price) external;

  function getSourceOfAsset(address asset) external view returns (address);
}