using NUnit.Framework;
using QuestDB.Buffers;

namespace net_questdb_client_tests;

public class BufferTests
{
    [Test]
    public void DecimalNegationSimple()
    {
        var buffer = new BufferV3(128, 128, 128);
        // Test simple negative decimal without carry propagation
        // -1.0m has unscaled value of -10 (with scale 1)
        buffer.Table("negation_test")
            .Symbol("tag", "simple")
            .Column("dec_neg_one", -1.0m)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // -10 in two's complement: 0xF6 (since 10 = 0x0A, ~0x0A + 1 = 0xF5 + 1 = 0xF6)
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_neg_one", 1, new byte[]
        {
            0xF6,
        });
    }

    [Test]
    public void DecimalNegationCarryLowToMid()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test carry propagation from low to mid part
        // Decimal with low=0x00000001, mid=0x00000000, high=0x00000000
        // After negation: low becomes 0xFFFFFFFF (overflow with carry), mid gets carry
        // This decimal is: -(2^96 / 10^28 + 1) which causes carry propagation
        // Use -4294967296 (which is -2^32) to force carry: low=0x00000000, mid=0x00000001
        // After negation: low = ~0 + 1 = 0, mid = ~1 + 1 = 0xFFFFFFFE + 1 = 0xFFFFFFFF
        var decimalValue = -4294967296m; // -2^32

        buffer.Table("negation_test")
            .Symbol("tag", "carry_low_mid")
            .Column("dec_carry", decimalValue)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // -4294967296 has bits: low=0, mid=1, high=0
        // Two's complement: low = ~0 + 1 = 0 (with carry), mid = ~1 + carry = 0xFFFFFFFE + 1 = 0xFFFFFFFF
        // Result should be: 0xFF, 0x00, 0x00, 0x00, 0x00 (compressed)
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_carry", 0, new byte[]
        {
            0xFF,
            0x00, 0x00, 0x00, 0x00,
        });
    }

    [Test]
    public void DecimalNegationCarryFullPropagation()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test carry propagation through all parts (low -> mid -> high)
        // Create a decimal where low=0, mid=0, high=1
        // This is 2^64 = 18446744073709551616
        // After negation: low = ~0 + 1 = 0 (carry), mid = ~0 + 1 = 0 (carry), high = ~1 + 1 = 0xFFFFFFFE + 1 = 0xFFFFFFFF
        var decimalValue = -18446744073709551616m; // -2^64

        buffer.Table("negation_test")
            .Symbol("tag", "carry_full")
            .Column("dec_full_carry", decimalValue)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // -18446744073709551616 has bits: low=0, mid=0, high=1
        // Two's complement propagates carry through all parts
        // Result: high=0xFFFFFFFF, mid=0, low=0
        // In big-endian with sign byte: 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_full_carry", 0, new byte[]
        {
            0xFF,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        });
    }

    [Test]
    public void DecimalNegationZeroEdgeCase()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test that -0.0m is treated as positive zero (line 92 in BufferV3.cs)
        // The code checks: var negative = (flags & SignMask) != 0 && value.Value != 0m;
        // This ensures that -0.0m doesn't get negated
        buffer.Table("negation_test")
            .Symbol("tag", "zero")
            .Column("dec_zero", 0.0m)
            .Column("dec_neg_zero", -0.0m)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // Both 0.0m and -0.0m should be encoded as positive zero: 0x00
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_zero", 1, new byte[]
        {
            0x00,
        });
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_neg_zero", 1, new byte[]
        {
            0x00,
        });
    }

    [Test]
    public void DecimalNegationSmallestValue()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test negation of the smallest representable positive value: 0.0000000000000000000000000001m
        // This has low=1, mid=0, high=0, scale=28
        // After negation: low = ~1 + 1 = 0xFFFFFFFE + 1 = 0xFFFFFFFF
        var decimalValue = -0.0000000000000000000000000001m;

        buffer.Table("negation_test")
            .Symbol("tag", "smallest")
            .Column("dec_smallest", decimalValue)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // Result should be: 0xFF (single byte, compressed)
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_smallest", 28, new byte[]
        {
            0xFF,
        });
    }

    [Test]
    public void DecimalNegationWithHighScale()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test negation with high scale value
        // -0.00000001m has scale=8
        var decimalValue = -0.00000001m; // -10^-8

        buffer.Table("negation_test")
            .Symbol("tag", "high_scale")
            .Column("dec_high_scale", decimalValue)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // -1 with scale 8 = 0xFF in two's complement
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_high_scale", 8, new byte[]
        {
            0xFF,
        });
    }

    [Test]
    public void DecimalNegationBoundaryCarry()
    {
        var buffer = new BufferV3(128, 128, 128);

        // Test a value where low=0xFFFFFFFF (all ones), which after negation becomes 1
        // This is the value 4294967295 (2^32 - 1)
        // After negation: low = ~0xFFFFFFFF + 1 = 0x00000000 + 1 = 0x00000001
        // So -4294967295 should give us: 0xFF, 0xFF, 0xFF, 0xFF, 0x01
        var decimalValue = -4294967295m;

        buffer.Table("negation_test")
            .Symbol("tag", "boundary")
            .Column("dec_boundary", decimalValue)
            .At(new DateTime(1970, 01, 01, 0, 0, 1));

        // Two's complement of 4294967295: negates to 0xFFFFFFFF00000001 (represented in big-endian)
        DecimalTestHelpers.AssertDecimalField(buffer.GetSendBuffer(), "dec_boundary", 0, new byte[]
        {
            0xFF,
            0xFF, 0xFF, 0xFF, 0x01,
        });
    }

}